"""Neo4j backend for the 'recgraph' (GraphRec) scenario — recommendations + fraud/identity over the e-commerce graph.

Recommendation queries traverse the co-purchase graph (Customer-PLACED-Order-CONTAINS-Product); fraud queries traverse
the shared-identifier graph (Customer-USED/FROM_DEVICE/SHIPS_TO-identifier). Seeding uses UNWIND batch MERGE; a uniqueness
constraint on every node key both enforces identity and provides the index the traversals seek on.
"""
from __future__ import annotations

import logging
import time
from typing import Any

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import Neo4jError

logging.getLogger("neo4j").setLevel(logging.ERROR)             # silence transient deadlock-retry chatter (retries succeed)
logging.getLogger("neo4j.notifications").setLevel(logging.ERROR)

from ..config import StackConfig
from .recgraph import RecGraphBackend

DB = "neo4j"


class Neo4jRecGraphBackend(RecGraphBackend):
    def __init__(self, cfg: StackConfig):
        self.cfg = cfg
        self.t = cfg.primary
        self.driver = None
        import os
        self._oid_base = 800_000_000_000 + (os.getpid() & 0xFFFFF) * 10_000_000    # unique per worker process
        self._oid_seq = 0

    # --- lifecycle -----------------------------------------------------------------------------
    def connect(self) -> None:
        self.driver = GraphDatabase.driver(f"bolt://{self.t.host}:{self.t.port}",
                                           auth=basic_auth(self.t.user, self.t.password) if self.t.user else None,
                                           connection_timeout=15, max_connection_pool_size=200, connection_acquisition_timeout=60)
        self.driver.verify_connectivity()

    def close(self) -> None:
        try:
            self.driver.close()
        except Exception:  # noqa: BLE001
            pass

    def _read(self, cypher: str, **p):
        with self.driver.session(database=DB, default_access_mode="READ") as s:
            return list(s.run(cypher, **p))

    def _write(self, cypher: str, **p):
        with self.driver.session(database=DB) as s:
            return s.execute_write(lambda tx: list(tx.run(cypher, **p)))

    def server_info(self) -> str:
        try:
            r = self._read("CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0], edition")
            return f"{r[0][0]} {r[0][1]} ({r[0][2]}) — recgraph"
        except Exception as e:  # noqa: BLE001
            return f"Neo4j (unknown: {e})"

    def create_schema(self) -> list[str]:
        stmts = [
            "CREATE CONSTRAINT rc_customer IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT rc_product IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT rc_category IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT rc_order IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT rc_card IF NOT EXISTS FOR (c:Card) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT rc_device IF NOT EXISTS FOR (d:Device) REQUIRE d.id IS UNIQUE",
            "CREATE CONSTRAINT rc_address IF NOT EXISTS FOR (a:Address) REQUIRE a.id IS UNIQUE",
            "CREATE INDEX rc_product_cat IF NOT EXISTS FOR (p:Product) ON (p.category_id)",
        ]
        for s in stmts:
            self._write(s)
        return stmts

    def drop_schema(self) -> None:
        # remove this scenario's nodes (shared labels with the base lab load — clear all so a re-seed is clean)
        for lbl in ("Customer", "Product", "Category", "Order", "Card", "Device", "Address", "Event", "Inventory"):
            while True:
                r = self._write(f"MATCH (n:{lbl}) WITH n LIMIT 50000 DETACH DELETE n RETURN count(n) AS c")
                if not r or r[0]["c"] == 0:
                    break
        for c in ("rc_customer", "rc_product", "rc_category", "rc_order", "rc_card", "rc_device", "rc_address"):
            try:
                self._write(f"DROP CONSTRAINT {c} IF EXISTS")
            except Neo4jError:
                pass

    def counts(self) -> dict[str, int]:
        out = {}
        for lbl in ("Customer", "Product", "Category", "Order", "Card", "Device", "Address"):
            out[lbl] = self._read(f"MATCH (n:{lbl}) RETURN count(n) AS c")[0]["c"]
        out["PLACED"] = self._read("MATCH ()-[r:PLACED]->() RETURN count(r) AS c")[0]["c"]
        out["CONTAINS"] = self._read("MATCH ()-[r:CONTAINS]->() RETURN count(r) AS c")[0]["c"]
        out["identity_edges"] = self._read("MATCH ()-[r:USED|FROM_DEVICE|SHIPS_TO]->() RETURN count(r) AS c")[0]["c"]
        return out

    # --- seed ----------------------------------------------------------------------------------
    def seed_categories(self, rows) -> int:
        self._write("UNWIND $rows AS r MERGE (c:Category {id:r.id}) SET c.name=r.name, c.depth=r.depth, c.parent_id=r.parent_id", rows=rows)
        self._write("UNWIND $rows AS r WITH r WHERE r.parent_id IS NOT NULL MATCH (c:Category {id:r.id}),(p:Category {id:r.parent_id}) MERGE (c)-[:CHILD_OF]->(p)", rows=rows)
        return len(rows)

    def seed_products(self, rows) -> int:
        self._write("UNWIND $rows AS r MERGE (p:Product {id:r.id}) SET p.name=r.name, p.price=r.price, p.active=r.active, p.category_id=r.category_id, p.tags=r.tags "
                    "WITH p, r MATCH (c:Category {id:r.category_id}) MERGE (p)-[:IN_CATEGORY]->(c)", rows=rows)
        return len(rows)

    def seed_customers(self, rows) -> int:
        self._write("UNWIND $rows AS r MERGE (c:Customer {id:r.id}) SET c.name=r.name, c.country=r.country, c.tier=r.tier", rows=rows)
        return len(rows)

    def create_identifier_pool(self, n_cards: int, n_devices: int, n_addrs: int) -> None:
        for lbl, n in (("Card", n_cards), ("Device", n_devices), ("Address", n_addrs)):
            for lo in range(1, n + 1, 20000):
                self._write(f"UNWIND range($lo, $hi) AS i MERGE (:{lbl} {{id:i}})", lo=lo, hi=min(lo + 19999, n))

    def seed_identifiers(self, rows) -> int:
        self._write("UNWIND $rows AS r MATCH (c:Customer {id:r.uid}) "
                    "FOREACH (x IN r.cards | MERGE (n:Card {id:x}) MERGE (c)-[:USED]->(n)) "
                    "FOREACH (x IN r.devices | MERGE (n:Device {id:x}) MERGE (c)-[:FROM_DEVICE]->(n)) "
                    "FOREACH (x IN r.addrs | MERGE (n:Address {id:x}) MERGE (c)-[:SHIPS_TO]->(n))", rows=rows)
        return len(rows)

    def seed_ratings(self, rows) -> int:
        self._write("UNWIND $rows AS r MATCH (c:Customer {id:r.uid}),(p:Product {id:r.product_id}) MERGE (c)-[rel:RATED]->(p) SET rel.stars=r.stars", rows=rows)
        return len(rows)

    def seed_orders(self, rows) -> int:
        self._write("UNWIND $rows AS r MERGE (o:Order {id:r.id}) SET o.ordered_at=r.ordered_at "
                    "WITH o, r MATCH (c:Customer {id:r.customer_id}) MERGE (c)-[:PLACED]->(o) "
                    "WITH o, r UNWIND r.items AS it MATCH (p:Product {id:it.product_id}) MERGE (o)-[rel:CONTAINS]->(p) SET rel.qty=it.qty", rows=rows)
        return len(rows)

    # --- recommendations -----------------------------------------------------------------------
    def also_bought(self, product_id, k=10):
        return [(r["id"], r["n"]) for r in self._read(
            "MATCH (p:Product {id:$pid})<-[:CONTAINS]-(:Order)<-[:PLACED]-(c:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(rec:Product) "
            "WHERE rec.id <> $pid RETURN rec.id AS id, count(*) AS n ORDER BY n DESC, id LIMIT $k", pid=product_id, k=k)]

    def for_you(self, customer_id, k=10):
        return [(r["id"], r["n"]) for r in self._read(
            "MATCH (me:Customer {id:$uid})-[:PLACED]->(:Order)-[:CONTAINS]->(mine:Product) "
            "WITH me, collect(DISTINCT mine) AS owned "
            "MATCH (me)-[:PLACED]->(:Order)-[:CONTAINS]->(p:Product)<-[:CONTAINS]-(:Order)<-[:PLACED]-(other:Customer)-[:PLACED]->(:Order)-[:CONTAINS]->(rec:Product) "
            "WHERE NOT rec IN owned AND other <> me RETURN rec.id AS id, count(*) AS n ORDER BY n DESC, id LIMIT $k", uid=customer_id, k=k)]

    def similar_products(self, product_id, k=10):
        return [(r["id"],) for r in self._read(
            "MATCH (p:Product {id:$pid})-[:IN_CATEGORY]->(cat)<-[:IN_CATEGORY]-(sim:Product) "
            "WHERE sim.id <> $pid AND sim.active RETURN sim.id AS id ORDER BY id LIMIT $k", pid=product_id, k=k)]

    def trending_in_category(self, category_id, k=10):
        return [(r["id"], r["n"]) for r in self._read(
            "MATCH (cat:Category {id:$cid})<-[:IN_CATEGORY]-(p:Product)<-[:CONTAINS]-(:Order) "
            "RETURN p.id AS id, count(*) AS n ORDER BY n DESC, id LIMIT $k", cid=category_id, k=k)]

    # --- fraud / identity ----------------------------------------------------------------------
    def fraud_ring(self, customer_id, k=20):
        return [(r["id"], r["shared"]) for r in self._read(
            "MATCH (c:Customer {id:$uid})-[:USED|FROM_DEVICE|SHIPS_TO]->(id)<-[:USED|FROM_DEVICE|SHIPS_TO]-(other:Customer) "
            "WHERE other.id <> $uid RETURN other.id AS id, count(DISTINCT id) AS shared ORDER BY shared DESC, id LIMIT $k", uid=customer_id, k=k)]

    def ring_expand(self, customer_id, depth=3, k=25):
        cy = ("MATCH (c:Customer {id:$uid}) "
              f"MATCH (c)-[:USED|FROM_DEVICE|SHIPS_TO*2..{2*depth}]-(other:Customer) "
              "WHERE other.id <> $uid RETURN DISTINCT other.id AS id LIMIT $k")
        return [(r["id"],) for r in self._read(cy, uid=customer_id, k=k)]

    def shortest_path(self, a, b, max_hops=6):
        if a == b:
            return 0
        r = self._read(
            f"MATCH (x:Customer {{id:$a}}),(y:Customer {{id:$b}}) "
            f"MATCH p = shortestPath((x)-[:USED|FROM_DEVICE|SHIPS_TO|PLACED|CONTAINS*..{max_hops}]-(y)) RETURN length(p) AS len", a=a, b=b)
        return r[0]["len"] if r else None

    def shared_id_accounts(self, customer_id):
        r = self._read(
            "MATCH (c:Customer {id:$uid})-[:USED|FROM_DEVICE|SHIPS_TO]->(id)<-[:USED|FROM_DEVICE|SHIPS_TO]-(other:Customer) "
            "WHERE other.id <> $uid RETURN count(DISTINCT other) AS n", uid=customer_id)
        return r[0]["n"] if r else 0

    # --- general traversal ---------------------------------------------------------------------
    def category_path(self, product_id):
        return [(r["depth"], r["cid"]) for r in self._read(
            "MATCH (p:Product {id:$pid})-[:IN_CATEGORY]->(c:Category) "
            "MATCH path=(c)-[:CHILD_OF*0..]->(root:Category) WHERE NOT (root)-[:CHILD_OF]->() "
            "WITH nodes(path) AS ns UNWIND range(0,size(ns)-1) AS i RETURN ns[i].depth AS depth, ns[i].id AS cid ORDER BY depth", pid=product_id)]

    def customer_orders(self, customer_id, limit=50):
        return [(r["oid"], r["items"]) for r in self._read(
            "MATCH (c:Customer {id:$uid})-[:PLACED]->(o:Order) OPTIONAL MATCH (o)-[:CONTAINS]->(p:Product) "
            "RETURN o.id AS oid, count(p) AS items ORDER BY oid DESC LIMIT $lim", uid=customer_id, lim=limit)]

    # --- writes --------------------------------------------------------------------------------
    def place_order(self, customer_id, product_ids):
        self._oid_seq += 1
        oid = self._oid_base + self._oid_seq
        self._write(
            "MATCH (c:Customer {id:$uid}) CREATE (o:Order {id:$oid, ordered_at:$ts}) MERGE (c)-[:PLACED]->(o) "
            "WITH o UNWIND $pids AS pid MATCH (p:Product {id:pid}) MERGE (o)-[r:CONTAINS]->(p) SET r.qty=1",
            uid=customer_id, oid=oid, ts=time.strftime("%Y-%m-%d %H:%M:%S"), pids=product_ids)
        return oid

    def rate_product(self, customer_id, product_id, stars):
        self._write("MATCH (c:Customer {id:$uid}),(p:Product {id:$pid}) MERGE (c)-[r:RATED]->(p) SET r.stars=$s",
                    uid=customer_id, pid=product_id, s=stars)

    # --- helper for the curves probe -----------------------------------------------------------
    def product_popularity(self, product_id) -> int:
        r = self._read("MATCH (:Order)-[c:CONTAINS]->(p:Product {id:$pid}) RETURN count(c) AS n", pid=product_id)
        return r[0]["n"] if r else 0
