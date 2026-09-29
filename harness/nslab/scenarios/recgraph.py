"""The 'recgraph' scenario ("GraphRec"): Neo4j's two flagship real-world workloads on the e-commerce graph —
product recommendations (co-purchase / collaborative filtering) and fraud-ring / identity-resolution detection.

Graph (property graph):
  (:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(:Product)-[:IN_CATEGORY]->(:Category)-[:CHILD_OF]->(:Category)
  (:Customer)-[:USED]->(:Card)   (:Customer)-[:FROM_DEVICE]->(:Device)   (:Customer)-[:SHIPS_TO]->(:Address)
  (:Customer)-[:RATED {stars}]->(:Product)
Identifiers (cards/devices/addresses) are drawn from a smaller Zipfian pool than the customer set, so many accounts
share a card/device/address — i.e. real fraud rings and hot identifiers exist to detect. Recommendations traverse the
co-purchase graph; fraud detection traverses the shared-identifier graph (variable-length). Base entity *values*
(customers, categories, products, orders+items) come from the shared deterministic `datagen`, so this scenario lines
up with the rest of the lab. Backend contract is engine-agnostic; the Neo4j implementation lives in neo4j_recgraph.py.
"""
from __future__ import annotations

import multiprocessing as mp
import random
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from .. import datagen
from ..util import summarize
from .base import Ctx, Op, Scenario, register


@dataclass(frozen=True)
class RecSizes:
    scale: float
    card_ratio: float = 0.8            # |cards|  = card_ratio * customers   (smaller pool => sharing => rings)
    device_ratio: float = 0.45
    addr_ratio: float = 0.6
    id_skew: float = 2.2               # Zipf skew for identifier sharing (higher => a few very shared ids / big rings)
    ratings_per_customer: int = 4

    @property
    def d(self):
        return datagen.Sizes.for_scale(self.scale)

    @classmethod
    def named(cls, name: str) -> "RecSizes":
        if name == "smoke":
            return cls(scale=0.05)
        if name == "small":
            return cls(scale=0.3)
        if name == "full":
            return cls(scale=1.0)
        raise ValueError(f"unknown size '{name}' (smoke|small|full)")


def _zipf(r: random.Random, n: int, skew: float) -> int:
    return 1 + int((n - 1) * (r.random() ** skew))


class RecCorpus:
    """Deterministic structure shared by seeder and driver (identifiers/ratings derived per-customer; base entities
    stream from datagen at seed time)."""

    def __init__(self, sizes: RecSizes):
        self.s = sizes
        d = sizes.d
        self.n_customers = d.customers
        self.n_products = d.products
        self.n_categories = d.categories
        self.n_orders = d.orders
        self.n_cards = max(1, int(d.customers * sizes.card_ratio))
        self.n_devices = max(1, int(d.customers * sizes.device_ratio))
        self.n_addrs = max(1, int(d.customers * sizes.addr_ratio))

    # identifiers a customer is attached to (Zipfian => shared across accounts)
    def cards_of(self, uid: int) -> list[int]:
        r = random.Random(uid * 2654435761 & 0xFFFFFFFF)
        return sorted({_zipf(r, self.n_cards, self.s.id_skew) for _ in range(1 + (r.random() < 0.35))})

    def devices_of(self, uid: int) -> list[int]:
        r = random.Random(uid * 40503 & 0xFFFFFFFF)
        return sorted({_zipf(r, self.n_devices, self.s.id_skew) for _ in range(1 + (r.random() < 0.5) + (r.random() < 0.2))})

    def addrs_of(self, uid: int) -> list[int]:
        r = random.Random(uid * 2246822519 & 0xFFFFFFFF)
        return sorted({_zipf(r, self.n_addrs, self.s.id_skew) for _ in range(1 + (r.random() < 0.3))})

    def ratings_of(self, uid: int) -> list[tuple[int, int]]:
        r = random.Random(uid * 7919 & 0xFFFFFFFF)
        return [(_zipf(r, self.n_products, 1.6), r.randint(1, 5)) for _ in range(self.s.ratings_per_customer)]

    def random_customer(self, r: random.Random) -> int:
        return _zipf(r, self.n_customers, 1.6)

    def random_product(self, r: random.Random) -> int:
        return _zipf(r, self.n_products, 1.8)     # low ids = popular (big co-purchase neighbourhoods)

    def random_category(self, r: random.Random) -> int:
        return r.randint(1, self.n_categories)


class RecGraphBackend(ABC):
    @abstractmethod
    def connect(self) -> None: ...
    @abstractmethod
    def close(self) -> None: ...
    @abstractmethod
    def create_schema(self) -> list[str]: ...
    @abstractmethod
    def drop_schema(self) -> None: ...
    @abstractmethod
    def server_info(self) -> str: ...
    @abstractmethod
    def counts(self) -> dict[str, int]: ...
    # seed (bulk)
    @abstractmethod
    def seed_customers(self, rows: list[dict]) -> int: ...
    @abstractmethod
    def seed_categories(self, rows: list[dict]) -> int: ...
    @abstractmethod
    def seed_products(self, rows: list[dict]) -> int: ...
    @abstractmethod
    def seed_orders(self, rows: list[dict]) -> int: ...        # {id, customer_id, ordered_at, items:[{product_id,qty}]}
    @abstractmethod
    def seed_identifiers(self, rows: list[dict]) -> int: ...   # {uid, cards:[], devices:[], addrs:[]}
    @abstractmethod
    def seed_ratings(self, rows: list[dict]) -> int: ...       # {uid, product_id, stars}
    # recommendations
    @abstractmethod
    def also_bought(self, product_id: int, k: int = 10) -> list[tuple]: ...
    @abstractmethod
    def for_you(self, customer_id: int, k: int = 10) -> list[tuple]: ...
    @abstractmethod
    def similar_products(self, product_id: int, k: int = 10) -> list[tuple]: ...
    @abstractmethod
    def trending_in_category(self, category_id: int, k: int = 10) -> list[tuple]: ...
    # fraud / identity
    @abstractmethod
    def fraud_ring(self, customer_id: int, k: int = 20) -> list[tuple]: ...
    @abstractmethod
    def ring_expand(self, customer_id: int, depth: int = 3, k: int = 25) -> list[tuple]: ...
    @abstractmethod
    def shortest_path(self, a: int, b: int, max_hops: int = 6) -> int | None: ...
    @abstractmethod
    def shared_id_accounts(self, customer_id: int) -> int: ...
    # general traversal
    @abstractmethod
    def category_path(self, product_id: int) -> list[tuple]: ...
    @abstractmethod
    def customer_orders(self, customer_id: int, limit: int = 50) -> list[tuple]: ...
    # writes
    @abstractmethod
    def place_order(self, customer_id: int, product_ids: list[int]) -> int: ...
    @abstractmethod
    def rate_product(self, customer_id: int, product_id: int, stars: int) -> None: ...


# --- operations (hotspot-skewed target selection from the corpus) --------------------------------
def op_also_bought(c: Ctx):
    return c.backend.also_bought(c.corpus.random_product(c.rng))

def op_for_you(c: Ctx):
    return c.backend.for_you(c.corpus.random_customer(c.rng))

def op_similar(c: Ctx):
    return c.backend.similar_products(c.corpus.random_product(c.rng))

def op_trending(c: Ctx):
    return c.backend.trending_in_category(c.corpus.random_category(c.rng))

def op_fraud_ring(c: Ctx):
    return c.backend.fraud_ring(c.corpus.random_customer(c.rng))

def op_ring_expand(c: Ctx):
    return c.backend.ring_expand(c.corpus.random_customer(c.rng), depth=c.rng.choice([2, 3, 4]))

def op_shortest_path(c: Ctx):
    return c.backend.shortest_path(c.corpus.random_customer(c.rng), c.corpus.random_customer(c.rng))

def op_shared_id(c: Ctx):
    return c.backend.shared_id_accounts(c.corpus.random_customer(c.rng))

def op_category_path(c: Ctx):
    return c.backend.category_path(c.corpus.random_product(c.rng))

def op_customer_orders(c: Ctx):
    return c.backend.customer_orders(c.corpus.random_customer(c.rng))

def op_place_order(c: Ctx):
    pids = sorted({c.corpus.random_product(c.rng) for _ in range(c.rng.randint(1, 4))})
    return c.backend.place_order(c.corpus.random_customer(c.rng), pids)

def op_rate(c: Ctx):
    return c.backend.rate_product(c.corpus.random_customer(c.rng), c.corpus.random_product(c.rng), c.rng.randint(1, 5))


DEFAULT_MIX: list[Op] = [
    Op("also_bought",          18, "read",  op_also_bought,   ("reco", "cf")),
    Op("for_you",              14, "read",  op_for_you,       ("reco", "personalised")),
    Op("similar_products",     10, "read",  op_similar,       ("reco",)),
    Op("trending_in_category",  6, "read",  op_trending,      ("reco", "agg")),
    Op("fraud_ring",           12, "read",  op_fraud_ring,    ("fraud", "identity")),
    Op("ring_expand",           6, "read",  op_ring_expand,   ("fraud", "varlen")),
    Op("shortest_path",         4, "read",  op_shortest_path, ("fraud", "path")),
    Op("shared_id_accounts",    6, "read",  op_shared_id,     ("fraud", "hotspot")),
    Op("category_path",         4, "read",  op_category_path, ("traversal",)),
    Op("customer_orders",       8, "read",  op_customer_orders,("serve",)),
    Op("place_order",           8, "write", op_place_order,   ("ingest",)),
    Op("rate_product",          4, "write", op_rate,          ("ingest",)),
]
MIX_BY_NAME = {o.name: o for o in DEFAULT_MIX}


def make_backend(stack_key: str, cfg, opts: dict | None = None) -> RecGraphBackend:
    if cfg.driver == "bolt":
        from .neo4j_recgraph import Neo4jRecGraphBackend
        return Neo4jRecGraphBackend(cfg)
    raise NotImplementedError(f"recgraph has no backend for driver '{cfg.driver}' (stack {stack_key}) yet")


# =============================== seed ==================================================
def _seed_worker(stack_key: str, size: str, w: int, workers: int, q: mp.Queue) -> None:
    from ..config import load_stack
    cfg = load_stack(stack_key)
    sizes = RecSizes.named(size)
    corpus = RecCorpus(sizes)
    be = make_backend(stack_key, cfg); be.connect()
    counts = {"customers": 0, "categories": 0, "products": 0, "orders": 0, "identifiers": 0, "ratings": 0}
    t0 = time.time()
    d = sizes.d
    rng = random.Random(datagen.SEED)
    try:
        # categories + products: cheap, seed entirely from worker 0 (they must exist before order/product edges)
        if w == 0:
            cats = [{"id": c["id"], "parent_id": c["parent_id"], "name": c["name"], "depth": c["depth"]}
                    for c in datagen.gen_categories(random.Random(1), d.categories)]
            counts["categories"] += be.seed_categories(cats)
            pbuf = []
            for p in datagen.gen_products(random.Random(2), d.products, d.categories):
                pbuf.append({"id": p["id"], "category_id": p["category_id"], "name": p["name"], "price": float(p["price"]),
                             "active": p["active"], "tags": (p.get("attrs") or {}).get("tags", [])})
                if len(pbuf) >= 2000:
                    counts["products"] += be.seed_products(pbuf); pbuf = []
            if pbuf:
                counts["products"] += be.seed_products(pbuf)
        # pre-create the identifier node pools (worker 0) so the sharded edge MERGEs contend far less
        if w == 0:
            be.create_identifier_pool(corpus.n_cards, corpus.n_devices, corpus.n_addrs)
        # customers + identifiers + ratings, sharded by uid
        cbuf, ibuf, rbuf = [], [], []
        cust = datagen.gen_customers(random.Random(3), d.customers)
        for cust_doc in cust:
            uid = cust_doc["id"]
            if uid % workers != w:
                continue
            cbuf.append({"id": uid, "name": cust_doc["name"], "country": cust_doc["country_code"], "tier": cust_doc["tier"]})
            ibuf.append({"uid": uid, "cards": corpus.cards_of(uid), "devices": corpus.devices_of(uid), "addrs": corpus.addrs_of(uid)})
            for pid, stars in corpus.ratings_of(uid):
                rbuf.append({"uid": uid, "product_id": pid, "stars": stars})
            if len(cbuf) >= 3000:
                counts["customers"] += be.seed_customers(cbuf); cbuf = []
                counts["identifiers"] += be.seed_identifiers(ibuf); ibuf = []
                counts["ratings"] += be.seed_ratings(rbuf); rbuf = []
        if cbuf:
            counts["customers"] += be.seed_customers(cbuf)
            counts["identifiers"] += be.seed_identifiers(ibuf)
            counts["ratings"] += be.seed_ratings(rbuf)
        # orders (+ CONTAINS to products), sharded by order id
        obuf = []
        prod_prices = [1.0] * (d.products + 1)
        for o in datagen.gen_orders_and_items(random.Random(4), d.orders, d.customers, d.products, prod_prices):
            if o["id"] % workers != w:
                continue
            obuf.append({"id": o["id"], "customer_id": o["customer_id"], "ordered_at": o["ordered_at"].strftime("%Y-%m-%d %H:%M:%S"),
                         "items": [{"product_id": it["product_id"], "qty": it["qty"]} for it in o["items"]]})
            if len(obuf) >= 1500:
                counts["orders"] += be.seed_orders(obuf); obuf = []
        if obuf:
            counts["orders"] += be.seed_orders(obuf)
        q.put({"w": w, "counts": counts, "seconds": round(time.time() - t0, 1), "error": None})
    except Exception as e:  # noqa: BLE001
        q.put({"w": w, "counts": counts, "seconds": round(time.time() - t0, 1), "error": f"{type(e).__name__}: {e}"[:300]})
    finally:
        be.close()


def seed(stack_key: str, size: str, workers: int = 6, *, log=print) -> dict[str, Any]:
    from ..config import load_stack
    cfg = load_stack(stack_key)
    be = make_backend(stack_key, cfg); be.connect()
    log("  dropping + creating graph schema ...")
    be.drop_schema()
    schema = be.create_schema()
    log(f"  schema: {len(schema)} constraints/indexes")
    be.close()
    sizes = RecSizes.named(size); d = sizes.d
    log(f"  seeding size={size}: {d.customers:,} customers, {d.products:,} products, {d.orders:,} orders, "
        f"~{RecCorpus(sizes).n_cards:,} cards / {RecCorpus(sizes).n_devices:,} devices (shared => fraud rings), {workers} workers")
    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    procs = [ctx.Process(target=_seed_worker, args=(stack_key, size, w, workers, q), daemon=True) for w in range(workers)]
    t0 = time.time()
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join(timeout=10)
    total = {"customers": 0, "categories": 0, "products": 0, "orders": 0, "identifiers": 0, "ratings": 0}
    errs = []
    for r in results:
        for k, v in r["counts"].items():
            total[k] += v
        if r["error"]:
            errs.append(r["error"])
    secs = time.time() - t0
    be = make_backend(stack_key, cfg); be.connect(); server = be.server_info(); cnt = be.counts(); be.close()
    out = {"size": size, "sizes": {"scale": sizes.scale, **{k: getattr(RecCorpus(sizes), k) for k in ("n_customers", "n_products", "n_orders", "n_cards", "n_devices", "n_addrs")}},
           "seconds": round(secs, 1), "written": total, "graph_counts": cnt, "errors": errs, "server": server}
    log(f"  seeded in {secs:.1f}s: {total}")
    log(f"  graph now: {cnt}")
    if errs:
        log(f"  seed errors: {errs[:2]}")
    return out


# =============================== analysis curves ==========================================
def curves(stack_key: str, size: str, opts: dict, *, samples: int = 300, log=print) -> dict[str, Any]:
    """recommendation latency vs product popularity; fraud ring-expansion latency vs traversal depth."""
    from ..config import load_stack
    cfg = load_stack(stack_key)
    corpus = RecCorpus(RecSizes.named(size))
    be = make_backend(stack_key, cfg); be.connect()
    rng = random.Random(11)
    log(f"  recommendation (also_bought) latency vs product popularity ({samples} samples) ...")
    reco_pts = []
    for _ in range(samples):
        pid = rng.randint(1, corpus.n_products)          # uniform over products so every popularity bucket fills
        t0 = time.perf_counter_ns()
        try:
            rows = be.also_bought(pid)
            reco_pts.append((be.product_popularity(pid), (time.perf_counter_ns() - t0) / 1e6))
        except Exception:  # noqa: BLE001
            pass
    log(f"  fraud ring_expand latency vs traversal depth ({samples} samples) ...")
    depth_pts = {2: [], 3: [], 4: []}
    for _ in range(samples):
        uid = corpus.random_customer(rng)
        depth = rng.choice([2, 3, 4])
        t0 = time.perf_counter_ns()
        try:
            be.ring_expand(uid, depth=depth)
            depth_pts[depth].append((time.perf_counter_ns() - t0) / 1e6)
        except Exception:  # noqa: BLE001
            pass
    be.close()

    def bucketize(pts, edges, labels):
        b = {lb: [] for lb in labels}
        for sz_, ms in pts:
            for e, lb in zip(edges, labels):
                if sz_ <= e:
                    b[lb].append(ms); break
            else:
                b[labels[-1]].append(ms)
        return {lb: {"n": len(v), **({k: round(val, 3) for k, val in summarize(v).items() if k in ("p50", "p95", "p99", "max")} if v else {})} for lb, v in b.items()}

    out = {"reco_vs_popularity": bucketize(reco_pts, [5, 25, 100, 500, 10**9], ["<=5", "6-25", "26-100", "101-500", "500+"]),
           "ring_expand_vs_depth": {f"{d}_hops": {"n": len(v), **({k: round(val, 3) for k, val in summarize(v).items() if k in ("p50", "p95", "p99", "max")} if v else {})} for d, v in depth_pts.items()}}
    log("  reco_vs_popularity: " + "  ".join(f"{lb}:{b.get('p95','-')}ms(n{b.get('n',0)})" for lb, b in out["reco_vs_popularity"].items()))
    log("  ring_expand_vs_depth: " + "  ".join(f"{lb}:{b.get('p95','-')}ms(n{b.get('n',0)})" for lb, b in out["ring_expand_vs_depth"].items()))
    return out


register(Scenario(
    name="recgraph",
    sizes_named=RecSizes.named,
    make_corpus=lambda s: RecCorpus(s),
    make_backend=make_backend,
    mix=lambda: DEFAULT_MIX,
    seed=seed,
    curves=curves,
    default_opts={},
    blurb="GraphRec — product recommendations (co-purchase) + fraud-ring / identity detection on the e-commerce graph.",
))
