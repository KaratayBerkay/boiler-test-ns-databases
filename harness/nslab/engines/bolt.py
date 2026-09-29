"""Bolt adapter: Neo4j 5 (and Memgraph via the same driver) — property-graph model over the operation catalog.

Graph model (labels / relationship types):
  (:Customer {id,...})  (:Category {id,...})  (:Product {id,...})  (:Order {id,...})  (:Event {id,...})
  (:Warehouse {id})     (:Inventory {product_id,warehouse_id,qty,version})
  (:Customer)-[:PLACED]->(:Order)-[:CONTAINS {qty,unit_price}]->(:Product)-[:IN_CATEGORY]->(:Category)-[:CHILD_OF]->(:Category)
  (:Customer)-[:DID]->(:Event)
Order items are relationships (Order-[:CONTAINS]->Product), so "referenced items" is native; the graph_traversal
and join operations are where a graph engine is meant to win. Full-text and vector use Neo4j's built-in indexes.
"""
from __future__ import annotations

import math
import time
from pathlib import Path
from typing import Any

from neo4j import GraphDatabase, basic_auth
from neo4j.exceptions import ClientError, Neo4jError

from ..config import Target
from ..datagen import read_docs
from ..ops import Unsupported
from .. import dockerctl
from .base import Client, Engine, EngineError, batched, iso

LABEL = {"customers": "Customer", "categories": "Category", "products": "Product", "orders": "Order",
         "events": "Event", "inventory": "Inventory", "order_items": "OrderItem", "repl_probe": "Probe", "scratch": "Scratch"}


class BoltClient(Client):
    def __init__(self, engine: "BoltEngine", target: Target, driver):
        super().__init__(engine, target, driver)
        self.db = target.database or "neo4j"

    def close(self) -> None:
        try:
            self.raw.close()
        except Exception:  # noqa: BLE001
            pass

    def run(self, cypher: str, **params):
        with self.raw.session(database=self.db, default_access_mode="WRITE" if self.target.role == "primary" else "READ") as s:
            return list(s.run(cypher, **params))

    def run_write(self, cypher: str, **params):
        with self.raw.session(database=self.db) as s:
            return s.execute_write(lambda tx: list(tx.run(cypher, **params)))

    # --- lifecycle -----------------------------------------------------------------------------
    def ping(self):
        return self.run("RETURN 1")

    def server_version(self) -> str:
        for runner in (self.run, self._sys):          # the lab database may not exist yet (cluster: created on load)
            try:
                r = runner("CALL dbms.components() YIELD name, versions, edition RETURN name, versions[0], edition")
                if r:
                    return f"{r[0][0]} {r[0][1]} ({r[0][2]})"
            except Exception:  # noqa: BLE001
                pass
        try:
            r = self.run("SHOW DATABASES YIELD name RETURN count(*)")
            return "Neo4j (version unavailable)"
        except Exception as e:  # noqa: BLE001
            return f"unknown ({e})"

    def role(self) -> str:
        """Single instance: 'primary'. Cluster: 'leader' / 'follower' for a direct member connection, 'leader (routed)' for a
        routing (neo4j://) target — read from SHOW DATABASE, matching the member's advertised Bolt address."""
        if self.engine.features.get("enterprise"):
            try:
                rows = self._sys("SHOW DATABASE $db YIELD address, role, writer, currentStatus RETURN address, role, writer, currentStatus", db=self.db)
                if self.target.extra.get("routing"):
                    return "leader (routed)" if any(r["writer"] for r in rows) else "no leader"
                adv = self.target.extra.get("advertised")
                me = next((r for r in rows if r["address"] == adv), None)
                if me:
                    return "leader" if me["writer"] else ("follower" if me["role"] == "primary" else "secondary")
            except Exception:  # noqa: BLE001
                pass
        try:
            self.run("SHOW SERVERS YIELD name, health, hosting RETURN health LIMIT 1")
            return "primary"
        except Exception:  # noqa: BLE001
            return self.target.role

    def _sys(self, cypher: str, **params):
        with self.raw.session(database="system") as s:
            return list(s.run(cypher, **params))

    # --- primitives --------------------------------------------------------------------------------
    def count(self, coll: str) -> int:
        lbl = LABEL[coll]
        r = self.run(f"MATCH (n:{lbl}) RETURN count(n) AS c")
        return int(r[0]["c"]) if r else 0

    def get(self, coll: str, key: Any) -> dict | None:
        lbl = LABEL[coll]
        r = self.run(f"MATCH (n:{lbl} {{id: $id}}) RETURN n", id=key)
        return dict(r[0]["n"]) if r else None

    def put(self, coll: str, doc: dict) -> None:
        self.bulk_put(coll, [doc])

    def delete(self, coll: str, key: Any) -> None:
        self.run_write(f"MATCH (n:{LABEL[coll]} {{id: $id}}) DETACH DELETE n", id=key)

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        cypher, rows = self.engine.write_cypher(coll, docs)
        self.run_write(cypher, rows=rows)
        return len(docs)

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        self.run_write(f"UNWIND $ks AS k MATCH (n:{LABEL[coll]} {{id: k}}) DETACH DELETE n", ks=keys)

    def drop_collection(self, coll: str) -> None:
        lbl = LABEL[coll]
        while True:
            r = self.run_write(f"MATCH (n:{lbl}) WITH n LIMIT 50000 DETACH DELETE n RETURN count(n) AS c")
            if not r or r[0]["c"] == 0:
                break

    # --- operations -------------------------------------------------------------------------------------
    def op_get_by_key(self, p):
        r = self.run("MATCH (c:Customer {id:$id}) RETURN c.id, c.name, c.email, c.country_code", id=p["id"])
        return [tuple(rec.values()) for rec in r]

    def op_multi_get(self, p):
        r = self.run("MATCH (c:Customer) WHERE c.id IN $ids RETURN c.id AS id, c.tier AS tier ORDER BY id", ids=p["ids"])
        return [(rec["id"], rec["tier"]) for rec in r]

    def op_insert_one(self, p):
        self.put("events", p)
        return None

    def op_update_field(self, p):
        self.run_write("MATCH (c:Customer {id:$id}) SET c.last_login = $v", id=p["id"], v=p["value"])
        return None

    def op_upsert(self, p):
        self.run_write("MERGE (i:Inventory {product_id:$pid, warehouse_id:$w}) SET i.qty=$q, i.updated_at=$t",
                       pid=p["product_id"], w=p["warehouse_id"], q=p["qty"], t=iso(__import__("datetime").datetime.utcnow()))
        return None

    def op_delete_one(self, p):
        self.run_write("MATCH (e:Event {id:$id}) DETACH DELETE e", id=p["id"])
        return None

    def op_atomic_increment(self, p):
        r = self.run_write("MATCH (i:Inventory {product_id:$pid, warehouse_id:$w}) SET i.qty = i.qty + 1 RETURN i.qty AS q",
                           pid=p["product_id"], w=p["warehouse_id"])
        return [(r[0]["q"],)] if r else []

    def op_conditional_update(self, p):
        r = self.run_write("MATCH (i:Inventory {product_id:$pid, warehouse_id:$w}) WITH i, i.version AS v "
                           "SET i.qty = 250, i.version = i.version + 1 RETURN v AS old, i.version AS new",
                           pid=p["product_id"], w=p["warehouse_id"])
        if not r:
            return [(0, -1)]
        return [(1, r[0]["new"])]

    def op_batch_insert_1000(self, p):
        self.bulk_put("events", p["docs"])
        return None

    def op_range_by_secondary(self, p):
        r = self.run("MATCH (c:Customer {id:$id})-[:PLACED]->(o:Order) RETURN o.id AS id, o.status AS status, o.total_amount AS amt ORDER BY id",
                     id=p["customer_id"])
        return [(rec["id"], rec["status"], rec["amt"]) for rec in r]

    def op_range_by_time(self, p):
        r = self.run("MATCH (e:Event {event_type:$t}) WHERE e.occurred_at >= $s AND e.occurred_at < $e "
                     "RETURN e.id AS id, e.customer_id AS cid, e.occurred_at AS ts ORDER BY ts, id LIMIT $lim",
                     t=p["event_type"], s=iso(p["start"]), e=iso(p["end"]), lim=p["limit"])
        return [(rec["id"], rec["cid"], rec["ts"]) for rec in r]

    def op_filter_nested(self, p):
        r = self.run("MATCH (c:Customer {tier:'pro', attrs_lang:'tr'}) RETURN count(c) AS c")
        return [(r[0]["c"],)]

    def op_array_contains(self, p):
        r = self.run("MATCH (p:Product) WHERE p.active AND 'eco' IN p.tags RETURN count(p) AS c")
        return [(r[0]["c"],)]

    def op_top_n_sorted(self, p):
        r = self.run("MATCH (p:Product {category_id:$cid}) WHERE p.active RETURN p.id AS id, p.price AS price ORDER BY price DESC, id ASC LIMIT $n",
                     cid=p["category_id"], n=p["n"])
        return [(rec["id"], rec["price"]) for rec in r]

    def op_pagination_offset(self, p):
        r = self.run("MATCH (o:Order) RETURN o.id AS id, o.ordered_at AS ts ORDER BY ts, id SKIP $off LIMIT $lim", off=p["offset"], lim=p["page"])
        return [(rec["id"], rec["ts"]) for rec in r]

    def op_pagination_keyset(self, p):
        cur = getattr(self, "_cursor", None)
        if cur is None:
            r = self.run("MATCH (o:Order) RETURN o.ordered_at AS ts, o.id AS id ORDER BY ts, id SKIP $off LIMIT 1", off=p["offset"] - 1)
            cur = self._cursor = (r[0]["ts"], r[0]["id"])
        r = self.run("MATCH (o:Order) WHERE o.ordered_at > $ts OR (o.ordered_at = $ts AND o.id > $id) "
                     "RETURN o.id AS id, o.ordered_at AS oat ORDER BY o.ordered_at, o.id LIMIT $lim", ts=cur[0], id=cur[1], lim=p["page"])
        return [(rec["id"], rec["oat"]) for rec in r]

    def op_count_filtered(self, p):
        r = self.run("MATCH (o:Order {status:'pending'}) RETURN count(o) AS c")
        return [(r[0]["c"],)]

    def op_aggregate_group(self, p):
        r = self.run("MATCH (o:Order) RETURN o.shipping_country AS c, count(*) AS n, sum(o.total_amount) AS t ORDER BY c")
        return [(rec["c"], rec["n"], round(rec["t"], 2)) for rec in r]

    def op_aggregate_time_bucket(self, p):
        r = self.run("MATCH (e:Event {event_type:'checkout'}) WITH substring(e.occurred_at,0,7) AS m, e.value_num AS v "
                     "RETURN m, count(*) AS n, sum(v) AS t ORDER BY m")
        return [(rec["m"], rec["n"], round(rec["t"], 4)) for rec in r]

    def op_join_lookup(self, p):
        r = self.run("MATCH (c:Customer {id:$id})-[:PLACED]->(o:Order) RETURN o.id AS id, c.name AS name, o.total_amount AS amt ORDER BY id", id=p["customer_id"])
        return [(rec["id"], rec["name"], rec["amt"]) for rec in r]

    def op_graph_traversal(self, p):
        # the native strength: variable-length path from a product up the category tree
        r = self.run("MATCH (p:Product {id:$id})-[:IN_CATEGORY]->(c:Category) "
                     "MATCH path = (c)-[:CHILD_OF*0..]->(root:Category) WHERE NOT (root)-[:CHILD_OF]->() "
                     "WITH nodes(path) AS ns UNWIND range(0, size(ns)-1) AS i RETURN ns[i].depth AS depth, ns[i].id AS cid ORDER BY depth", id=p["product_id"])
        return [(rec["depth"], rec["cid"]) for rec in r]

    def op_text_search(self, p):
        if self.engine.text_index:
            q = " AND ".join(p["words"])
            r = self.run("CALL db.index.fulltext.queryNodes('product_desc', $q) YIELD node RETURN node.id AS id", q=q)
            return sorted((rec["id"],) for rec in r)[:p["limit"]]
        words = p["words"]
        r = self.run("MATCH (p:Product) WHERE all(w IN $ws WHERE p.description CONTAINS w) RETURN p.id AS id ORDER BY id LIMIT $lim", ws=words, lim=p["limit"])
        return [(rec["id"],) for rec in r]

    def op_geo_near(self, p):
        r = self.run("WITH point({latitude:$lat, longitude:$lon}) AS q MATCH (c:Customer) WHERE c.loc IS NOT NULL "
                     "WITH c, point.distance(c.loc, q) AS d WHERE d <= $m RETURN c.id AS id ORDER BY d LIMIT $lim",
                     lat=p["lat"], lon=p["lon"], m=p["km"] * 1000, lim=p["limit"])
        return [(rec["id"],) for rec in r]

    def op_vector_knn(self, p):
        if not self.engine.vector:
            raise Unsupported("no vector index")
        r = self.run("CALL db.index.vector.queryNodes('product_vec', $k, $v) YIELD node RETURN node.id AS id", k=p["k"], v=p["vector"])
        return [(rec["id"],) for rec in r]

    def op_transaction_transfer(self, p):
        self.run_write("MATCH (a:Inventory {product_id:$pid, warehouse_id:$f}), (b:Inventory {product_id:$pid, warehouse_id:$t}) "
                       "SET a.qty = a.qty - $amt, b.qty = b.qty + $amt", pid=p["product_id"], f=p["from"], t=p["to"], amt=p["amount"])
        return None

    def op_ttl_set(self, p):
        # Neo4j has no native TTL; store an expiry property (an application/APOC job would sweep it)
        self.run_write("MERGE (s:Scratch {id:$k}) SET s.expires_at = timestamp() + $ms", k=p["key"], ms=p["seconds"] * 1000)
        return None

    def op_scan_full(self, p):
        r = self.run("MATCH (e:Event) RETURN count(e) AS c")
        return [(r[0]["c"],)]

    # --- optimisation hooks -----------------------------------------------------------------------------
    def opt_fetch_full(self, coll, key):
        r = self.run(f"MATCH (n:{LABEL[coll]} {{id:$id}}) RETURN n", id=key)
        return [(dict(r[0]["n"]).get("id"),)] if r else []

    def opt_fetch_projected(self, coll, key, fields):
        proj = ", ".join(f"n.{f}" for f in fields)
        r = self.run(f"MATCH (n:{LABEL[coll]} {{id:$id}}) RETURN {proj}", id=key)
        return [tuple(r[0].values())] if r else []

    def opt_order_embedded(self, order_id):
        # "embedded": all item data is on the CONTAINS relationships of this order
        r = self.run("MATCH (o:Order {id:$id})-[c:CONTAINS]->() RETURN o.id AS id, count(c) AS n", id=order_id)
        return [(r[0]["id"], r[0]["n"])] if r else []

    def opt_order_referenced(self, order_id):
        r = self.run("MATCH (o:Order {id:$id})-[c:CONTAINS]->(p:Product) RETURN o.id AS id, count(p) AS n", id=order_id)
        return [(r[0]["id"], r[0]["n"])] if r else []

    def opt_write_durable(self, level, doc):
        self.put("events", doc)
        return None

    def opt_read_level(self, level, key):
        return self.op_get_by_key({"id": key})

    # --- probes -----------------------------------------------------------------------------------------------
    STATIC_PROBES = {
        "secondary_index": ("supported", "CREATE INDEX on a node property (range/point/text/lookup)"),
        "compound_index": ("supported", "CREATE INDEX ... FOR (n:L) ON (n.a, n.b) composite index"),
        "server_side_join": ("supported", "traversals ARE the join: MATCH patterns over relationships (index-free adjacency)"),
        "graph_traversal": ("supported", "variable-length relationships MATCH (a)-[:R*1..5]->(b); shortestPath; APOC"),
        "bulk_import": ("supported", "UNWIND $rows batches (used here); LOAD CSV; neo4j-admin database import (offline, fastest)"),
        "aggregation": ("supported", "count/sum/avg/collect + WITH grouping"),
        "nested_query": ("supported", "map properties / flattened; APOC for deep maps"),
        "pubsub": ("unsupported", "no pub/sub; change data capture (CDC) in Enterprise"),
    }

    def probe_unique_constraint(self):
        self.run_write("CREATE CONSTRAINT scratch_u IF NOT EXISTS FOR (s:Scratch) REQUIRE s.uk IS UNIQUE")
        self.run_write("MERGE (s:Scratch {id:'u1'}) SET s.uk = 1")
        try:
            self.run_write("CREATE (s:Scratch {id:'u2', uk:1})")
        except Neo4jError as e:
            return f"UNIQUE constraint rejects duplicate: {str(e)[:60]}"
        finally:
            self.run_write("MATCH (s:Scratch) WHERE s.id IN ['u1','u2'] DETACH DELETE s")
            self.run_write("DROP CONSTRAINT scratch_u IF EXISTS")
        raise RuntimeError("duplicate accepted")

    def probe_ttl_expiry(self):
        raise Unsupported("no native per-node TTL; APOC apoc.ttl or a scheduled job on an expiry property")

    def probe_text_index(self):
        if not self.engine.text_index:
            raise Unsupported("full-text index not created")
        self.op_text_search({"words": ["wireless"], "limit": 1})
        return "CREATE FULLTEXT INDEX + db.index.fulltext.queryNodes (Lucene)"

    def probe_geo_index(self):
        r = self.run("WITH point({latitude:52.5, longitude:13.4}) AS q MATCH (c:Customer) WHERE c.loc IS NOT NULL "
                     "RETURN c.id ORDER BY point.distance(c.loc, q) LIMIT 1")
        return "point type + point.distance; POINT range index for bounding-box"

    def probe_vector_index(self):
        if not self.engine.vector:
            raise Unsupported("vector index not created")
        self.op_vector_knn({"vector": [0.1] * 8, "k": 1})
        return "CREATE VECTOR INDEX (HNSW) + db.index.vector.queryNodes"

    def probe_partial_update(self):
        self.run_write("MATCH (c:Customer {id:1}) SET c.probe = 1")
        self.run_write("MATCH (c:Customer {id:1}) REMOVE c.probe")
        return "SET / REMOVE a single property"

    def probe_atomic_increment(self):
        return f"SET n.x = n.x + 1 inside an auto-committed transaction -> {self.op_atomic_increment({'product_id':1,'warehouse_id':1})}"

    def probe_optimistic_concurrency(self):
        return f"read version, SET guarded by WHERE in one tx -> {self.op_conditional_update({'product_id':1,'warehouse_id':1})}"

    def probe_multi_doc_transaction(self):
        with self.raw.session(database=self.db) as s:
            tx = s.begin_transaction()
            tx.run("CREATE (s:Scratch {id:'tx1'})")
            tx.rollback()
        if self.run("MATCH (s:Scratch {id:'tx1'}) RETURN s"):
            raise RuntimeError("rollback did not undo")
        return "full ACID transactions (multi-statement, commit/rollback)"

    def probe_tunable_consistency(self):
        raise Unsupported("single-primary within a cluster; routing (WRITE->leader, READ->followers) but no per-write quorum knob")

    def probe_array_operators(self):
        self.run_write("MATCH (p:Product {id:1}) SET p.tags = p.tags + 'probe'")
        r = self.run("MATCH (p:Product {id:1}) WHERE 'probe' IN p.tags RETURN count(p) AS c")
        self.run_write("MATCH (p:Product {id:1}) SET p.tags = [x IN p.tags WHERE x <> 'probe']")
        return f"list properties + IN / list comprehensions / UNWIND -> {r[0]['c']}"

    def probe_schema_validation(self):
        try:
            self.run_write("CREATE CONSTRAINT scratch_ex IF NOT EXISTS FOR (s:Scratch) REQUIRE s.req IS NOT NULL")
            self.run_write("DROP CONSTRAINT scratch_ex IF EXISTS")
            return "existence + type + key constraints"
        except Neo4jError as e:
            if "enterprise" in str(e).lower() or "not supported" in str(e).lower():
                raise Unsupported("existence/type/key constraints are Enterprise-only; Community has UNIQUE constraints only")
            raise

    def probe_change_streams(self):
        raise Unsupported("CDC is Enterprise-only (db.cdc.*); Community has no change feed")

    def probe_server_side_scripting(self):
        try:
            r = self.run("RETURN apoc.version() AS v")
            return f"APOC procedures/functions + custom user-defined procedures (Java) -> APOC {r[0]['v']}"
        except Neo4jError:
            return "user-defined procedures/functions in Java (CALL); APOC not installed here"

    def probe_pubsub(self):
        raise Unsupported("no pub/sub")

    def probe_explain_plan(self):
        r = self.run("EXPLAIN MATCH (c:Customer {id:1})-[:PLACED]->(o:Order) RETURN o")
        return "EXPLAIN / PROFILE returns the operator tree (NodeIndexSeek, Expand, ...)"

    def probe_large_document(self):
        blob = "x" * (15 * 1024 * 1024)
        self.run_write("MERGE (s:Scratch {id:'big'}) SET s.blob = $b", b=blob)
        r = self.run("MATCH (s:Scratch {id:'big'}) RETURN size(s.blob) AS n")
        self.run_write("MATCH (s:Scratch {id:'big'}) DELETE s")
        return f"{r[0]['n']/1048576:.0f} MB string property ok (default max is bounded by page/heap, not a fixed doc size)"

    def probe_many_fields(self):
        props = {f"f{i}": i for i in range(5000)}
        self.run_write("MERGE (s:Scratch {id:'wide'}) SET s += $p", p=props)
        r = self.run("MATCH (s:Scratch {id:'wide'}) RETURN size(keys(s)) AS n")
        self.run_write("MATCH (s:Scratch {id:'wide'}) DELETE s")
        return f"{r[0]['n']} properties on one node"

    def probe_read_from_replica(self):
        raise Unsupported("single instance in this lab (Enterprise causal cluster routes reads to secondaries via bolt+routing)")

    def probe_snapshot_backup(self):
        raise Unsupported("Community: neo4j-admin database dump needs the database stopped (STOP DATABASE is rejected, so the server stops); online backup is Enterprise")

    def probe_slow_query_log(self):
        r = self.run("CALL db.stats.status() YIELD section, status RETURN section, status")
        st = {x["section"]: x["status"] for x in r}
        return f"query.log (db.logs.query.threshold) stays empty in Community; the in-memory query collector db.stats.collect/retrieve('QUERIES') records every query with elapsed time (status: {st.get('QUERIES')})"

    STATIC_PROBES = dict(STATIC_PROBES, **{
        "audit_log": ("unsupported", "security.log auth events and query.log are Enterprise; Community has the query collector only"),
        "structured_logs": ("supported", "log4j2 (conf/server-logs.xml, user-logs.xml): switch the PatternLayout to JsonTemplateLayout; text by default"),
        "runtime_log_level": ("supported", "conf/server-logs.xml is re-read every 30 s (log4j2 monitorInterval) — edit the Root level, no restart; dbms.setConfigValue is Enterprise"),
        "incremental_backup": ("unsupported", "differential backups are Enterprise (neo4j-admin database backup --type=differential)"),
        "pitr": ("unsupported", "Enterprise: full + differential backups restore to a transaction id; Community has full dumps only"),
    })


class BoltEngine(Engine):
    driver = "bolt"
    load_workers = 1
    load_batch = 5000

    def apply_features(self, features: dict) -> None:
        self.vector = bool(features.get("vector", True))
        self.text_index = bool(features.get("text_index", True))
        if features.get("enterprise"):
            self.BACKUP_STRATEGY = self.BACKUP_STRATEGY_ENTERPRISE
            self.LOGGING_STRATEGY = dict(self.LOGGING_STRATEGY, slow_query="db.logs.query.enabled=INFO + db.logs.query.threshold -> /logs/query.log on every member (Enterprise: works; parameters, page hits, planning time per query)",
                                         audit="/logs/security.log (authentication / authorization events, Enterprise)",
                                         runtime="CALL dbms.setConfigValue for dynamic settings (db.logs.query.threshold ...) — no restart")

    def connect(self, target: Target, *, timeout: float = 10.0) -> BoltClient:
        auth = basic_auth(target.user, target.password) if target.user else None
        scheme = "neo4j" if target.extra.get("routing") else "bolt"        # neo4j:// = routing table, writes go to the leader
        driver = GraphDatabase.driver(f"{scheme}://{target.host}:{target.port}", auth=auth, connection_timeout=timeout,
                                      max_connection_pool_size=200, connection_acquisition_timeout=60)
        driver.verify_connectivity()
        return BoltClient(self, target, driver)

    def ensure_database(self, client: BoltClient) -> None:
        """Enterprise: the dataset lives in its own database (created on demand) so restores can DROP + re-seed it."""
        if self.features.get("enterprise") and client.db != "neo4j":
            client._sys(f"CREATE DATABASE `{client.db}` IF NOT EXISTS WAIT")

    def write_cypher(self, coll: str, docs: list[dict]) -> tuple[str, list[dict]]:
        if coll == "customers":
            rows = [{"id": d["id"], "name": d["name"], "email": d["email"], "country_code": d["country_code"], "tier": d["tier"],
                     "created_at": iso(d["created_at"]), "attrs_lang": (d.get("attrs") or {}).get("lang"),
                     "attrs_age": (d.get("attrs") or {}).get("age"), "tags": [], "lat": (d.get("location") or {}).get("lat"),
                     "lon": (d.get("location") or {}).get("lon")} for d in docs]
            return ("UNWIND $rows AS r MERGE (c:Customer {id:r.id}) SET c.name=r.name, c.email=r.email, c.country_code=r.country_code, "
                    "c.tier=r.tier, c.created_at=r.created_at, c.attrs_lang=r.attrs_lang, c.attrs_age=r.attrs_age, "
                    "c.loc = CASE WHEN r.lat IS NULL THEN NULL ELSE point({latitude:r.lat, longitude:r.lon}) END", rows)
        if coll == "categories":
            return ("UNWIND $rows AS r MERGE (c:Category {id:r.id}) SET c.name=r.name, c.depth=r.depth, c.parent_id=r.parent_id "
                    "WITH c, r WHERE r.parent_id IS NOT NULL MATCH (p:Category {id:r.parent_id}) MERGE (c)-[:CHILD_OF]->(p)", docs)
        if coll == "products":
            rows = [{"id": d["id"], "category_id": d["category_id"], "sku": d["sku"], "name": d["name"], "description": d["description"],
                     "price": float(d["price"]), "active": d["active"], "tags": (d.get("attrs") or {}).get("tags", []),
                     "attrs_color": (d.get("attrs") or {}).get("color"), "embedding": d.get("embedding")} for d in docs]
            return ("UNWIND $rows AS r MERGE (p:Product {id:r.id}) SET p.category_id=r.category_id, p.sku=r.sku, p.name=r.name, "
                    "p.description=r.description, p.price=r.price, p.active=r.active, p.tags=r.tags, p.attrs_color=r.attrs_color, p.embedding=r.embedding "
                    "WITH p, r MATCH (c:Category {id:r.category_id}) MERGE (p)-[:IN_CATEGORY]->(c)", rows)
        if coll == "orders":
            rows = [{"id": d["id"], "customer_id": d["customer_id"], "status": d["status"], "ordered_at": iso(d["ordered_at"]),
                     "total_amount": float(d["total_amount"]), "shipping_country": d["shipping_country"],
                     "items": [{"product_id": it["product_id"], "qty": it["qty"], "unit_price": float(it["unit_price"])} for it in d.get("items", [])]} for d in docs]
            return ("UNWIND $rows AS r MERGE (o:Order {id:r.id}) SET o.customer_id=r.customer_id, o.status=r.status, o.ordered_at=r.ordered_at, "
                    "o.total_amount=r.total_amount, o.shipping_country=r.shipping_country "
                    "WITH o, r MATCH (c:Customer {id:r.customer_id}) MERGE (c)-[:PLACED]->(o) "
                    "WITH o, r UNWIND r.items AS it MATCH (p:Product {id:it.product_id}) MERGE (o)-[rel:CONTAINS {item_product:it.product_id}]->(p) "
                    "SET rel.qty=it.qty, rel.unit_price=it.unit_price", rows)
        if coll == "order_items":
            return ("", [])          # items are edges created with orders; skip the flat collection
        if coll == "events":
            rows = [{"id": d["id"], "customer_id": d["customer_id"], "event_type": d["event_type"], "occurred_at": iso(d["occurred_at"]),
                     "value_num": float(d["value_num"])} for d in docs]
            return ("UNWIND $rows AS r MERGE (e:Event {id:r.id}) SET e.customer_id=r.customer_id, e.event_type=r.event_type, "
                    "e.occurred_at=r.occurred_at, e.value_num=r.value_num "
                    "WITH e, r MATCH (c:Customer {id:r.customer_id}) MERGE (c)-[:DID]->(e)", rows)
        if coll == "inventory":
            return ("UNWIND $rows AS r MERGE (i:Inventory {product_id:r.product_id, warehouse_id:r.warehouse_id}) "
                    "SET i.qty=r.qty, i.updated_at=r.updated_at, i.version=r.version",
                    [{"product_id": d["product_id"], "warehouse_id": d["warehouse_id"], "qty": d["qty"], "updated_at": iso(d["updated_at"]), "version": d.get("version", 1)} for d in docs])
        if coll == "repl_probe":
            return ("UNWIND $rows AS r MERGE (p:Probe {id:r.id}) SET p.ts_ms=r.ts_ms", docs)
        raise Unsupported(f"no mapping for {coll}")

    def collections(self) -> list[str]:
        return ["customers", "categories", "products", "orders", "events", "inventory"]   # items are edges

    # --- backup / restore ------------------------------------------------------------------------------
    BACKUP_STRATEGY = {
        "tool": "neo4j-admin database dump (single-file archive) — Community has no online backup, and STOP DATABASE is rejected, so the server is stopped for the dump",
        "consistency": "transactionally consistent (store closed while dumping)",
        "restore": "offline: stop, neo4j-admin database load --overwrite-destination=true, start (both run as the neo4j user in a helper container over the data volume)",
        "incremental": "Enterprise only (neo4j-admin database backup --type=differential)",
        "pitr": "Enterprise only (transaction-log based); Community = last full dump",
        "schedule": "a maintenance window per dump (cron: stop, dump, start) — or move to Enterprise for online full/differential backups",
        "retention": "external: dump files under /backups/<tag>/",
    }

    def _vols(self, t: Target) -> tuple[str, str, str]:
        mounts = {m.get("Destination"): m.get("Name") for m in dockerctl.inspect(t.container).get("Mounts", []) if m.get("Name")}
        if "/data" not in mounts or "/backups" not in mounts:
            raise EngineError(f"{t.container}: expected named volumes at /data and /backups, got {mounts}")
        image = dockerctl.inspect(t.container).get("Config", {}).get("Image") or self.cfg.image
        return mounts["/data"], mounts["/backups"], image

    def data_bytes(self, client: BoltClient) -> int | None:
        t = self.cfg.primary
        if not t.container:
            return None
        db = dockerctl.du_bytes(t.container, f"/data/databases/{t.database or 'neo4j'}") or 0
        tx = dockerctl.du_bytes(t.container, f"/data/transactions/{t.database or 'neo4j'}") or 0
        return db + tx or None

    def _offline(self, t: Target, cmd: list[str], *, log=print) -> tuple[float, str]:
        """Stop the server, run neo4j-admin over the volumes in a helper container (as neo4j), start, wait for Bolt."""
        data_vol, bk_vol, image = self._vols(t)
        t0 = time.perf_counter()
        dockerctl.container_action(t.container, "stop")
        rc, out, err = dockerctl.run_oneshot(image, cmd, volumes={data_vol: "/data", bk_vol: "/backups"}, user="neo4j", entrypoint="neo4j-admin", timeout=1800)
        if rc != 0:
            dockerctl.container_action(t.container, "start")
            raise EngineError(f"neo4j-admin {' '.join(cmd[:2])} rc={rc}: {(err or out)[-400:]}")
        dockerctl.container_action(t.container, "start")
        dockerctl.wait_for(lambda: self.connect(t, timeout=3).ping(), timeout=300, desc="neo4j back")
        return round((time.perf_counter() - t0) * 1000, 1), (out + err)[-400:]

    BACKUP_STRATEGY_ENTERPRISE = {
        "tool": "neo4j-admin database backup --from=<member>:6362 (online, from any member; --type=full|differential) into the shared /backups volume",
        "consistency": "transactionally consistent (the member's transaction log is included; `backup --type=differential` chains on the last full)",
        "restore": "cluster-wide re-seed: DROP DATABASE lab; CREATE DATABASE lab OPTIONS {existingData:'use', seedURI:'file:///backups/<tag>/<file>.backup'} WAIT — every primary seeds from the same file",
        "incremental": "differential backups (--type=differential) and `neo4j-admin database aggregate-backup` to fold them",
        "pitr": "transaction-log based (restore a full + differential chain up to a transaction id) — Enterprise",
        "schedule": "cron/CronJob running the online backup against a follower; no downtime",
        "retention": "external: prune backup directories, aggregate chains",
    }

    def _net(self, t: Target) -> str:
        nets = dockerctl.inspect(t.container).get("NetworkSettings", {}).get("Networks", {})
        return next(iter(nets), "bridge")

    def _backup_enterprise(self, client: BoltClient, tag: str, *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        db = t.database or "neo4j"
        dockerctl.exec_in(t.container, ["sh", "-c", f"mkdir -p /backups/{tag} && chmod 777 /backups/{tag}"], user="0", timeout=30)
        _, bk_vol, image = self._vols(t)
        svc = dockerctl.inspect(t.container).get("Config", {}).get("Labels", {}).get("com.docker.compose.service") or "neo4j1"
        # a helper container on the stack's network (compose) / a pod in the namespace (k8s) mounting only the backups volume
        rc, out, err = dockerctl.run_oneshot(image, ["database", "backup", db, f"--from={svc}:6362", f"--to-path=/backups/{tag}", "--type=full"],
                                             volumes={bk_vol: "/backups"}, user="neo4j", entrypoint="neo4j-admin", network=self._net(t),
                                             env={"NEO4J_ACCEPT_LICENSE_AGREEMENT": "eval"}, timeout=1800)
        if rc != 0:
            raise EngineError(f"neo4j-admin database backup rc={rc}: {(err or out)[-400:]}")
        p = type("R", (), {"stdout": out, "stderr": err})()
        rc, out, _ = dockerctl.exec_in(t.container, ["sh", "-c", f"ls /backups/{tag}"], timeout=30)
        files = out.split()
        size = dockerctl.du_bytes(t.container, f"/backups/{tag}")
        return {"method": f"neo4j-admin database backup {db} --from={svc}:6362 --type=full (online, Enterprise)", "online": True,
                "artifact": f"/backups/{tag}/{files[0] if files else '?'}", "artifact_bytes": size, "files": files, "tool_output": (p.stdout + p.stderr)[-200:]}

    def _restore_enterprise(self, client: BoltClient, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        db = t.database or "neo4j"
        rc, out, _ = dockerctl.exec_in(t.container, ["sh", "-c", f"ls /backups/{tag}"], timeout=30)
        files = [f for f in out.split() if f.endswith(".backup")]
        if not files:
            raise EngineError(f"no .backup file under /backups/{tag}")
        uri = f"file:///backups/{tag}/{files[-1]}"
        t0 = time.perf_counter()
        client._sys(f"DROP DATABASE `{db}` IF EXISTS WAIT")
        t_drop = time.perf_counter()
        client._sys(f"CREATE DATABASE `{db}` OPTIONS {{existingData: 'use', seedURI: '{uri}'}} WAIT")
        dockerctl.wait_for(lambda: self.connect(t, timeout=3).run("RETURN 1"), timeout=300, desc=f"database {db} online after seed")
        return {"method": "DROP DATABASE + CREATE DATABASE ... OPTIONS {existingData:'use', seedURI:'file://.../<tag>.backup'} WAIT (every primary seeds from the shared file)",
                "online": False, "downtime_ms": round((time.perf_counter() - t0) * 1000, 1), "drop_ms": round((t_drop - t0) * 1000, 1), "seed_uri": uri}

    def backup(self, client: BoltClient, tag: str, *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("no container (Aura/managed): use the provider's snapshots")
        if self.features.get("enterprise"):
            return self._backup_enterprise(client, tag, log=log)
        db = t.database or "neo4j"
        dockerctl.exec_in(t.container, ["sh", "-c", f"mkdir -p /backups/{tag} && chmod 777 /backups/{tag}"], user="0", timeout=30)
        downtime, tail = self._offline(t, ["database", "dump", db, f"--to-path=/backups/{tag}"], log=log)
        size = dockerctl.du_bytes(t.container, f"/backups/{tag}/{db}.dump")
        return {"method": "stop server; neo4j-admin database dump; start (Community: offline)", "online": False, "downtime_ms": downtime,
                "artifact": f"/backups/{tag}/{db}.dump", "artifact_bytes": size, "tool_output": tail[-200:]}

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        return [(self.cfg.primary.container, f"/backups/{tag}")]

    def restore(self, client: BoltClient, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        db = t.database or "neo4j"
        if self.features.get("enterprise"):
            return self._restore_enterprise(client, tag, info, log=log)
        downtime, tail = self._offline(t, ["database", "load", db, f"--from-path=/backups/{tag}", "--overwrite-destination=true"], log=log)
        return {"method": "stop server; neo4j-admin database load --overwrite-destination; start", "online": False, "downtime_ms": downtime, "tool_output": tail[-200:]}

    # --- logging -----------------------------------------------------------------------------------------
    LOGGING_STRATEGY = {
        "server_log": "/logs/neo4j.log (user log, also stdout), /logs/debug.log (root logger, 20 MB x 7 rolling), query.log / security.log / http.log — log4j2 via conf/server-logs.xml + user-logs.xml",
        "slow_query": "db.logs.query.enabled + db.logs.query.threshold write query.log with parameters/page hits — Enterprise only in practice (accepted but empty in Community); Community: CALL db.stats.collect/retrieve('QUERIES') in-memory query collector",
        "audit": "security.log (authentication/authorization events) is Enterprise; no statement audit in Community",
        "runtime": "log4j2 monitorInterval=30: edit conf/server-logs.xml and the level changes within 30 s, no restart; dbms.setConfigValue is Enterprise",
    }

    def server_log_facts(self, client: BoltClient) -> dict[str, Any]:
        t = self.cfg.primary
        if not t.container:
            return {"format": "text", "where": "server.directories.logs"}
        rc, out, _ = dockerctl.exec_in(t.container, ["sh", "-c", "ls -la /logs | tail -n +2; tail -n 2 /logs/neo4j.log"], timeout=60)
        lines = out.splitlines()
        return {"format": "text (log4j2 PatternLayout; JsonTemplateLayout available)", "where": "/logs/{neo4j,debug,query,security,http}.log + stdout",
                "files": [ln[:120] for ln in lines if ".log" in ln][:8], "sample": [ln[:200] for ln in lines[-2:]]}

    def slow_query_setup(self, client: BoltClient, threshold_ms: int) -> str:
        if self.features.get("enterprise"):
            client.run("CALL dbms.setConfigValue('db.logs.query.threshold', $v)", v=f"{threshold_ms}ms")
            self._qlog_before = {}
            for t in self.cfg.targets:
                if t.container:
                    rc, o, _ = dockerctl.exec_in(t.container, ["sh", "-c", "wc -l < /logs/query.log 2>/dev/null || echo 0"], timeout=30)
                    self._qlog_before[t.container] = int(o.strip() or 0)
            return f"CALL dbms.setConfigValue('db.logs.query.threshold', '{threshold_ms}ms') (dynamic) -> /logs/query.log on the member that ran the query"
        client.run("CALL db.stats.stop('QUERIES')")
        client.run("CALL db.stats.clear('QUERIES')")
        client.run("CALL db.stats.collect('QUERIES')")
        return (f"db.logs.query.threshold={threshold_ms}ms is configured but Community writes nothing to query.log; using the query collector: "
                f"CALL db.stats.clear/collect('QUERIES') (in-memory, every query with elapsed µs; filter >= {threshold_ms} ms)")

    def slow_query_fast_op(self, client: BoltClient) -> str:
        client.run("MATCH (c:Customer {id: $id}) RETURN c.name", id=1)
        return "MATCH (c:Customer {id: $id}) RETURN c.name"

    def slow_query_slow_op(self, client: BoltClient) -> str:
        client.run("MATCH (e:Event) WHERE e.value_num > 0.5 RETURN count(e)")
        return "MATCH (e:Event) WHERE e.value_num > 0.5 RETURN count(e) (1M-node label scan)"

    def slow_query_entries(self, client: BoltClient, threshold_ms: int) -> list[dict[str, Any]]:
        if self.features.get("enterprise"):
            import re
            out = []
            for t in self.cfg.targets:
                if not t.container or t.container in {x.get("c") for x in out}:
                    continue
                n0 = getattr(self, "_qlog_before", {}).get(t.container, 0)
                rc, o, _ = dockerctl.exec_in(t.container, ["sh", "-c", f"tail -n +{n0 + 1} /logs/query.log 2>/dev/null | tail -n 40"], timeout=30)
                for ln in o.splitlines():
                    m = re.search(r"\s(\d+) ms: ", ln)
                    if m and int(m.group(1)) >= threshold_ms:
                        out.append({"ms": int(m.group(1)), "text": ln.strip()[:300], "c": t.container.replace("nslab-", "")})
            out.sort(key=lambda x: -x["ms"])
            return out
        client.run("CALL db.stats.stop('QUERIES')")
        rows = client.run("CALL db.stats.retrieve('QUERIES') YIELD data RETURN data")
        out = []
        for r in rows:
            d = r["data"]
            inv = d.get("invocations") or []
            worst = max((i.get("elapsedExecutionTimeInUs", 0) for i in inv), default=0) / 1000
            if worst >= threshold_ms:
                out.append({"ms": round(worst, 1), "text": str(d.get("query"))[:200], "invocations": len(inv),
                            "compile_ms": round(max((i.get("elapsedCompileTimeInUs", 0) for i in inv), default=0) / 1000, 1)})
        out.sort(key=lambda x: -x["ms"])
        # what the query.log file holds (Community: nothing)
        t = self.cfg.primary
        if t.container:
            rc, o, _ = dockerctl.exec_in(t.container, ["sh", "-c", "wc -c < /logs/query.log"], timeout=30)
            out.append({"ms": None, "text": f"[/logs/query.log] {o.strip()} bytes (db.logs.query.enabled=INFO, threshold {threshold_ms}ms — Enterprise feature, empty in Community)", "kind": "server-log"})
        return out

    def slow_query_teardown(self, client: BoltClient) -> None:
        if self.features.get("enterprise"):
            client.run("CALL dbms.setConfigValue('db.logs.query.threshold', '100ms')")
            return
        client.run("CALL db.stats.collect('QUERIES')")

    def audit_log_setup(self, client: BoltClient) -> str:
        if not self.features.get("enterprise"):
            raise Unsupported("security.log (authentication/authorization events) and query.log are Enterprise; Community has no audit log")
        t = self.cfg.primary
        rc, o, _ = dockerctl.exec_in(t.container, ["sh", "-c", "wc -l < /logs/security.log 2>/dev/null || echo 0"], timeout=30)
        self._seclog_before = int(o.strip() or 0)
        return "/logs/security.log (Enterprise): authentication and authorization events; provoking a failed login + a denied write"

    def audit_log_exercise(self, client: BoltClient) -> list[str]:
        t = self.cfg.primary
        done = []
        try:
            d = GraphDatabase.driver(f"bolt://{t.host}:{t.port}", auth=basic_auth("neo4j", "wrong-password"), connection_timeout=5)
            d.verify_connectivity()
        except Exception as e:  # noqa: BLE001
            done.append(f"login neo4j/<wrong> -> {type(e).__name__}")
        try:
            client._sys("CREATE USER nslab_reader IF NOT EXISTS SET PASSWORD 'r3ader' CHANGE NOT REQUIRED")
            client._sys("GRANT ROLE reader TO nslab_reader")
            d = GraphDatabase.driver(f"bolt://{t.host}:{t.port}", auth=basic_auth("nslab_reader", "r3ader"), connection_timeout=5)
            try:
                with d.session(database=client.db) as sess:
                    sess.run("CREATE (:AuditProbe {id: 1})").consume()
            except Exception as e:  # noqa: BLE001
                done.append(f"CREATE as reader -> {type(e).__name__}")
            finally:
                d.close()
        except Exception as e:  # noqa: BLE001
            done.append(f"reader setup failed: {type(e).__name__}: {str(e)[:60]}")
        return done

    def audit_log_entries(self, client: BoltClient) -> list[str]:
        t = self.cfg.primary
        n0 = getattr(self, "_seclog_before", 0)
        rc, o, _ = dockerctl.exec_in(t.container, ["sh", "-c", f"tail -n +{n0 + 1} /logs/security.log 2>/dev/null | tail -n 20"], timeout=30)
        return [ln.strip()[:240] for ln in o.splitlines() if ln.strip()]

    def audit_log_teardown(self, client: BoltClient) -> None:
        try:
            client._sys("DROP USER nslab_reader IF EXISTS")
        except Exception:  # noqa: BLE001
            pass

    def log_level_runtime(self, client: BoltClient) -> str:
        if self.features.get("enterprise"):
            client.run("CALL dbms.setConfigValue('db.logs.query.threshold', '250ms')")
            seen = client.run("SHOW SETTINGS YIELD name, value WHERE name = 'db.logs.query.threshold' RETURN value")[0]["value"]
            client.run("CALL dbms.setConfigValue('db.logs.query.threshold', '100ms')")
            if seen != "250ms":
                raise EngineError(f"setting not applied: {seen}")
            return "CALL dbms.setConfigValue('db.logs.query.threshold', '250ms') -> SHOW SETTINGS verified -> back to 100ms (dynamic, no restart); log4j2 monitorInterval reload also available"
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("needs access to conf/server-logs.xml")
        conf = "/var/lib/neo4j/conf/server-logs.xml"
        rc, before, _ = dockerctl.exec_in(t.container, ["sh", "-c", "grep -c ' DEBUG ' /logs/debug.log || true"], timeout=30)
        rc, out, err = dockerctl.exec_in(t.container, ["sed", "-i", 's/<Root level="INFO">/<Root level="DEBUG">/', conf], user="neo4j", timeout=30)
        if rc != 0:
            raise EngineError(f"edit {conf}: {err[-200:]}")
        try:
            t0 = time.perf_counter()
            def _debug_seen():
                client.run("RETURN 1")
                rc2, now, _ = dockerctl.exec_in(t.container, ["sh", "-c", "grep -c ' DEBUG ' /logs/debug.log || true"], timeout=30)
                return int(now.strip() or 0) > int(before.strip() or 0)
            dockerctl.wait_for(_debug_seen, timeout=75, interval=2.0, desc="DEBUG lines in debug.log after log4j2 reload")
            waited = round(time.perf_counter() - t0, 1)
        finally:
            dockerctl.exec_in(t.container, ["sed", "-i", 's/<Root level="DEBUG">/<Root level="INFO">/', conf], user="neo4j", timeout=30)
        return f"edited conf/server-logs.xml Root level INFO->DEBUG; log4j2 monitorInterval=30 picked it up after {waited}s (DEBUG lines appeared in debug.log); reverted"

    def reset(self, client: BoltClient) -> None:
        self.ensure_database(client)
        for lbl in set(LABEL.values()):
            while True:
                r = client.run_write(f"MATCH (n:{lbl}) WITH n LIMIT 50000 DETACH DELETE n RETURN count(n) AS c")
                if not r or r[0]["c"] == 0:
                    break

    def create_schema(self, client: BoltClient) -> list[str]:
        stmts = [
            "CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT category_id IF NOT EXISTS FOR (c:Category) REQUIRE c.id IS UNIQUE",
            "CREATE CONSTRAINT product_id IF NOT EXISTS FOR (p:Product) REQUIRE p.id IS UNIQUE",
            "CREATE CONSTRAINT order_id IF NOT EXISTS FOR (o:Order) REQUIRE o.id IS UNIQUE",
            "CREATE CONSTRAINT event_id IF NOT EXISTS FOR (e:Event) REQUIRE e.id IS UNIQUE",
            "CREATE CONSTRAINT inv_key IF NOT EXISTS FOR (i:Inventory) REQUIRE (i.product_id, i.warehouse_id) IS UNIQUE",
            "CREATE INDEX cust_country_tier IF NOT EXISTS FOR (c:Customer) ON (c.country_code, c.tier)",
            "CREATE INDEX prod_cat IF NOT EXISTS FOR (p:Product) ON (p.category_id)",
            "CREATE INDEX order_status_ts IF NOT EXISTS FOR (o:Order) ON (o.status, o.ordered_at)",
            "CREATE INDEX order_ts IF NOT EXISTS FOR (o:Order) ON (o.ordered_at)",
            "CREATE INDEX event_type IF NOT EXISTS FOR (e:Event) ON (e.event_type)",
            "CREATE POINT INDEX cust_loc IF NOT EXISTS FOR (c:Customer) ON (c.loc)",
        ]
        done = []
        for s in stmts:
            client.run_write(s)
            done.append(s.split("FOR")[0].strip())
        if self.text_index:
            try:
                client.run_write("CREATE FULLTEXT INDEX product_desc IF NOT EXISTS FOR (p:Product) ON EACH [p.description]")
                done.append("FULLTEXT INDEX product_desc")
            except Neo4jError as e:
                self.text_index = False
                done.append(f"fulltext FAILED: {str(e)[:80]}")
        if self.vector:
            try:
                client.run_write("CREATE VECTOR INDEX product_vec IF NOT EXISTS FOR (p:Product) ON (p.embedding) "
                                 "OPTIONS {indexConfig: {`vector.dimensions`: 8, `vector.similarity_function`: 'cosine'}}")
                done.append("VECTOR INDEX product_vec (HNSW, cosine, dim 8)")
            except Neo4jError as e:
                self.vector = False
                done.append(f"vector FAILED: {str(e)[:80]}")
        return done

    def after_load(self, client: BoltClient) -> list[str]:
        try:
            client.run_write("CALL db.awaitIndexes(120)")
        except Neo4jError:
            pass
        return ["indexes online"]

    def create_index(self, client: BoltClient, name: str, coll: str, fields: list[str]) -> str:
        if coll == "events":
            client.run_write("CREATE INDEX ev_type_ts IF NOT EXISTS FOR (e:Event) ON (e.event_type, e.occurred_at)")
            client.run_write("CALL db.awaitIndexes(120)")
            return "composite range index Event(event_type, occurred_at)"
        raise Unsupported("only the events composite index is modelled")

    def drop_index(self, client: BoltClient, name: str, coll: str) -> None:
        try:
            client.run_write("DROP INDEX ev_type_ts IF EXISTS")
        except Neo4jError:
            pass

    def index_variants(self):
        return [("no_index", None), ("composite_(event_type,occurred_at)", ["event_type", "occurred_at"])]

    def max_connections(self, client: BoltClient) -> int | None:
        try:
            r = client.run("SHOW SETTINGS YIELD name, value WHERE name = 'server.bolt.thread_pool_max_size' RETURN value")
            return int(r[0]["value"]) if r else None
        except Neo4jError:
            return None

    def replication_status(self, client: BoltClient) -> dict[str, Any]:
        try:
            cols = "address, role, writer, currentStatus, lastCommittedTxn, replicationLag" if self.features.get("enterprise") else "address, role, currentStatus"
            r = client._sys(f"SHOW DATABASE $db YIELD {cols} RETURN {cols}", db=client.db)
            members = [dict(x) for x in r]
            out: dict[str, Any] = {"members": members}
            leader = next((m["address"] for m in members if m.get("writer")), None)
            if leader:
                out["leader"] = leader
            return out
        except Neo4jError as e:
            return {"note": "single instance (Community)", "err": str(e)[:80]}

    def leader_target(self) -> Target | None:
        """Cluster: the direct-Bolt target whose advertised address is the current writer (Raft leader) of the lab database."""
        if not self.features.get("enterprise"):
            return None
        c = self.connect_primary(timeout=10)
        try:
            rows = c._sys("SHOW DATABASE $db YIELD address, writer RETURN address, writer", db=c.db)
        finally:
            c.close()
        leader = next((r["address"] for r in rows if r["writer"]), None)
        for t in self.cfg.targets:
            if t.container and t.extra.get("advertised") == leader:
                return t
        return None

    def promote(self, target: Target) -> str:
        return "Raft leader election among the remaining primaries (automatic)" if self.features.get("enterprise") else "n/a (single instance)"
