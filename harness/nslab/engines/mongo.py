"""MongoDB wire-protocol adapter: MongoDB (replica set), FerretDB 2, DocumentDB (Microsoft) — via pymongo.

Documents are stored as-is with `_id` = the dataset id (inventory: "<product_id>:<warehouse_id>"); customers.location
is converted to GeoJSON so a 2dsphere index applies. Order items stay embedded in `orders.items`; the flat
`order_items` collection is loaded too (opt06 compares the two shapes).
"""
from __future__ import annotations

import datetime as dt
import json
import time
from pathlib import Path
from typing import Any

import pymongo
from pymongo import ASCENDING, DESCENDING, MongoClient, ReadPreference, WriteConcern
from pymongo.errors import OperationFailure, PyMongoError

from ..config import Target
from ..datagen import read_docs
from ..ops import Unsupported
from .. import dockerctl
from .base import Client, Engine, EngineError, batched, iso

INV = "inventory"


def inv_id(pid: int, w: int) -> str:
    return f"{pid}:{w}"


def prep(coll: str, d: dict) -> dict:
    d = dict(d)
    if coll == INV:
        d["_id"] = inv_id(d["product_id"], d["warehouse_id"])
    else:
        d["_id"] = d["id"]
    if coll == "customers" and d.get("location"):
        loc = d["location"]
        d["location"] = {"type": "Point", "coordinates": [loc["lon"], loc["lat"]]}
    return d


class MongoClientAdapter(Client):
    def __init__(self, engine: "MongoEngine", target: Target, raw: MongoClient):
        super().__init__(engine, target, raw)
        if target.role == "replica":
            self.db = raw.get_database(target.database or "lab", read_preference=ReadPreference.SECONDARY_PREFERRED)
        else:
            self.db = raw[target.database or "lab"]

    def c(self, coll: str):
        return self.db[coll]

    # --- lifecycle ---------------------------------------------------------------------
    def ping(self):
        return self.raw.admin.command("ping")

    def server_version(self) -> str:
        try:
            bi = self.raw.server_info()
            v = bi.get("version", "?")
            extra = ""
            try:
                h = self.raw.admin.command("hello")
                if "ferretdb" in str(h).lower() or "ferretdb" in str(bi).lower():
                    extra = " (FerretDB)"
                elif "documentdb" in str(bi).lower():
                    extra = " (DocumentDB)"
            except Exception:  # noqa: BLE001
                pass
            return f"MongoDB {v}{extra}"
        except Exception as e:  # noqa: BLE001
            return f"unknown ({e})"

    def role(self) -> str:
        try:
            h = self.raw.admin.command("hello")
            if h.get("isWritablePrimary") or h.get("ismaster"):
                return "primary"
            if h.get("secondary"):
                return "replica"
            return "unknown"
        except Exception:  # noqa: BLE001
            return "unknown"

    # --- primitives ---------------------------------------------------------------------------
    def count(self, coll: str) -> int:
        return self.c(coll).count_documents({})

    def get(self, coll: str, key: Any) -> dict | None:
        return self.c(coll).find_one({"_id": key})

    def put(self, coll: str, doc: dict) -> None:
        d = prep(coll, doc)
        self.c(coll).replace_one({"_id": d["_id"]}, d, upsert=True)

    def delete(self, coll: str, key: Any) -> None:
        self.c(coll).delete_one({"_id": key})

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        self.c(coll).insert_many([prep(coll, d) for d in docs], ordered=False)
        return len(docs)

    def load_bulk_put(self, coll: str, docs: list[dict]) -> int:
        self.c(coll).with_options(write_concern=WriteConcern(w=1)).insert_many([prep(coll, d) for d in docs], ordered=False)
        return len(docs)

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        self.c(coll).delete_many({"_id": {"$in": keys}})

    def drop_collection(self, coll: str) -> None:
        self.c(coll).drop()

    # --- operations ----------------------------------------------------------------------------
    def op_get_by_key(self, p):
        d = self.c("customers").find_one({"_id": p["id"]}, {"id": 1, "name": 1, "email": 1, "country_code": 1})
        return self.rows([d], ("id", "name", "email", "country_code")) if d else []

    def op_multi_get(self, p):
        cur = self.c("customers").find({"_id": {"$in": p["ids"]}}, {"id": 1, "tier": 1}).sort("_id", 1)
        return self.rows(cur, ("id", "tier"))

    def op_insert_one(self, p):
        self.c("events").insert_one(prep("events", p))
        return None

    def op_update_field(self, p):
        self.c("customers").update_one({"_id": p["id"]}, {"$set": {"last_login": p["value"]}})
        return None

    def op_upsert(self, p):
        self.c(INV).update_one({"_id": inv_id(p["product_id"], p["warehouse_id"])},
                               {"$set": {"product_id": p["product_id"], "warehouse_id": p["warehouse_id"], "qty": p["qty"],
                                         "updated_at": dt.datetime.utcnow()}, "$setOnInsert": {"version": 1}}, upsert=True)
        return None

    def op_delete_one(self, p):
        self.c("events").delete_one({"_id": p["id"]})
        return None

    def op_atomic_increment(self, p):
        d = self.c(INV).find_one_and_update({"_id": inv_id(p["product_id"], p["warehouse_id"])}, {"$inc": {"qty": 1}},
                                            return_document=pymongo.ReturnDocument.AFTER, projection={"qty": 1})
        return [(d["qty"],)] if d else []

    def op_conditional_update(self, p):
        _id = inv_id(p["product_id"], p["warehouse_id"])
        cur = self.c(INV).find_one({"_id": _id}, {"version": 1})
        if not cur:
            return [(0, -1)]
        r = self.c(INV).update_one({"_id": _id, "version": cur["version"]}, {"$set": {"qty": 250}, "$inc": {"version": 1}})
        return [(r.modified_count, cur["version"] + r.modified_count)]

    def op_batch_insert_1000(self, p):
        self.c("events").insert_many([prep("events", d) for d in p["docs"]], ordered=False)
        return None

    def op_range_by_secondary(self, p):
        cur = self.c("orders").find({"customer_id": p["customer_id"]}, {"id": 1, "status": 1, "total_amount": 1}).sort("_id", 1)
        return self.rows(cur, ("id", "status", "total_amount"))

    def op_range_by_time(self, p):
        cur = (self.c("events").find({"event_type": p["event_type"], "occurred_at": {"$gte": p["start"], "$lt": p["end"]}},
                                     {"id": 1, "customer_id": 1, "occurred_at": 1})
               .sort([("occurred_at", 1), ("_id", 1)]).limit(p["limit"]))
        return [(d["id"], d["customer_id"], iso(d["occurred_at"])) for d in cur]

    def op_filter_nested(self, p):
        return [(self.c("customers").count_documents({"attrs.lang": "tr", "tier": "pro"}),)]

    def op_array_contains(self, p):
        return [(self.c("products").count_documents({"attrs.tags": "eco", "active": True}),)]

    def op_top_n_sorted(self, p):
        cur = self.c("products").find({"category_id": p["category_id"], "active": True}, {"id": 1, "price": 1}).sort([("price", -1), ("_id", 1)]).limit(p["n"])
        return self.rows(cur, ("id", "price"))

    def op_pagination_offset(self, p):
        cur = self.c("orders").find({}, {"id": 1, "ordered_at": 1}).sort([("ordered_at", 1), ("_id", 1)]).skip(p["offset"]).limit(p["page"])
        return [(d["id"], iso(d["ordered_at"])) for d in cur]

    def op_pagination_keyset(self, p):
        cur_key = getattr(self, "_cursor", None)
        if cur_key is None:
            last = list(self.c("orders").find({}, {"id": 1, "ordered_at": 1}).sort([("ordered_at", 1), ("_id", 1)]).skip(p["offset"] - 1).limit(1))[0]
            cur_key = self._cursor = (last["ordered_at"], last["_id"])
        ts, _id = cur_key
        cur = (self.c("orders").find({"$or": [{"ordered_at": {"$gt": ts}}, {"ordered_at": ts, "_id": {"$gt": _id}}]}, {"id": 1, "ordered_at": 1})
               .sort([("ordered_at", 1), ("_id", 1)]).limit(p["page"]))
        return [(d["id"], iso(d["ordered_at"])) for d in cur]

    def op_count_filtered(self, p):
        return [(self.c("orders").count_documents({"status": "pending"}),)]

    def op_aggregate_group(self, p):
        cur = self.c("orders").aggregate([{"$group": {"_id": "$shipping_country", "n": {"$sum": 1}, "total": {"$sum": "$total_amount"}}},
                                          {"$sort": {"_id": 1}}])
        return [(d["_id"], d["n"], round(d["total"], 2)) for d in cur]

    def op_aggregate_time_bucket(self, p):
        cur = self.c("events").aggregate([
            {"$match": {"event_type": "checkout"}},
            {"$group": {"_id": {"$dateToString": {"format": "%Y-%m", "date": "$occurred_at"}}, "n": {"$sum": 1}, "total": {"$sum": "$value_num"}}},
            {"$sort": {"_id": 1}}])
        return [(d["_id"], d["n"], round(d["total"], 4)) for d in cur]

    def op_join_lookup(self, p):
        cur = self.c("orders").aggregate([
            {"$match": {"customer_id": p["customer_id"]}},
            {"$lookup": {"from": "customers", "localField": "customer_id", "foreignField": "_id", "as": "cust"}},
            {"$unwind": "$cust"},
            {"$project": {"id": 1, "name": "$cust.name", "total_amount": 1}},
            {"$sort": {"_id": 1}}])
        return [(d["id"], d["name"], d["total_amount"]) for d in cur]

    def op_graph_traversal(self, p):
        cur = self.c("products").aggregate([
            {"$match": {"_id": p["product_id"]}},
            {"$graphLookup": {"from": "categories", "startWith": "$category_id", "connectFromField": "parent_id",
                              "connectToField": "_id", "as": "path", "depthField": "d"}},
            {"$unwind": "$path"},
            {"$project": {"depth": "$path.depth", "cid": "$path._id"}},
            {"$sort": {"depth": 1}}])
        return [(d["depth"], d["cid"]) for d in cur]

    def op_text_search(self, p):
        q = " ".join(f'"{w}"' for w in p["words"])
        cur = self.c("products").find({"$text": {"$search": q}}, {"id": 1}).sort("_id", 1).limit(p["limit"])
        return self.rows(cur, ("id",))

    def op_geo_near(self, p):
        cur = self.c("customers").find({"location": {"$nearSphere": {"$geometry": {"type": "Point", "coordinates": [p["lon"], p["lat"]]},
                                                                      "$maxDistance": p["km"] * 1000}}}, {"id": 1}).limit(p["limit"])
        return self.rows(cur, ("id",))

    def op_vector_knn(self, p):
        try:
            cur = self.c("products").aggregate([{"$vectorSearch": {"index": "vec_products", "path": "embedding", "queryVector": p["vector"],
                                                                   "numCandidates": 100, "limit": p["k"]}}, {"$project": {"id": 1}}])
            return self.rows(cur, ("id",))
        except OperationFailure as e:
            raise Unsupported(f"$vectorSearch: {str(e)[:120]}")

    def op_transaction_transfer(self, p):
        a, b = inv_id(p["product_id"], p["from"]), inv_id(p["product_id"], p["to"])
        with self.raw.start_session() as s:
            def txn(sess):
                self.c(INV).update_one({"_id": a}, {"$inc": {"qty": -p["amount"]}}, session=sess)
                self.c(INV).update_one({"_id": b}, {"$inc": {"qty": p["amount"]}}, session=sess)
            s.with_transaction(txn)
        return None

    def op_ttl_set(self, p):
        self.c("scratch").replace_one({"_id": p["key"]}, {"_id": p["key"], "v": p["value"],
                                                          "expires_at": dt.datetime.utcnow() + dt.timedelta(seconds=p["seconds"])}, upsert=True)
        return None

    def op_scan_full(self, p):
        return [(self.c("events").count_documents({}),)]


    # --- optimisation hooks -------------------------------------------------------------------------
    def opt_fetch_full(self, coll, key):
        d = self.c(coll).find_one({"_id": key})
        return [(d["_id"], len(d))] if d else []

    def opt_fetch_projected(self, coll, key, fields):
        d = self.c(coll).find_one({"_id": key}, {f: 1 for f in fields})
        return [tuple(d.get(f) for f in fields)] if d else []

    def opt_order_embedded(self, order_id):
        d = self.c("orders").find_one({"_id": order_id})
        return [(d["id"], len(d.get("items", [])))]

    def opt_order_referenced(self, order_id):
        d = self.c("orders").find_one({"_id": order_id}, {"items": 0})
        items = list(self.c("order_items").find({"order_id": order_id}, {"id": 1, "product_id": 1, "qty": 1}))
        return [(d["id"], len(items))]

    _WC = {"w1": WriteConcern(w=1), "w1_journal": WriteConcern(w=1, j=True), "w_majority": WriteConcern(w="majority", j=False),
           "w_majority_journal": WriteConcern(w="majority", j=True), "w0_unacknowledged": WriteConcern(w=0)}

    def opt_write_durable(self, level, doc):
        self.c("events").with_options(write_concern=self._WC[level]).insert_one(prep("events", doc))
        return None

    def opt_read_level(self, level, key):
        if level in ("primary", "secondaryPreferred", "nearest"):
            coll = self.c("customers").with_options(read_preference=getattr(ReadPreference, level.upper() if level != "secondaryPreferred" else "SECONDARY_PREFERRED"))
        else:
            from pymongo.read_concern import ReadConcern
            coll = self.c("customers").with_options(read_concern=ReadConcern(level))
        d = coll.find_one({"_id": key}, {"id": 1, "name": 1})
        return [(d["id"], d["name"])] if d else []

    # --- probes --------------------------------------------------------------------------------------
    STATIC_PROBES = {
        "secondary_index": ("supported", "createIndex on any field"),
        "compound_index": ("supported", "compound index (ESR rule: equality, sort, range)"),
        "bulk_import": ("supported", "insertMany / bulkWrite / mongoimport"),
        "server_side_join": ("supported", "$lookup (+ $graphLookup)"),
        "aggregation": ("supported", "aggregation pipeline"),
        "nested_query": ("supported", "dot notation on embedded documents"),
        "graph_traversal": ("supported", "$graphLookup"),
    }

    def probe_unique_constraint(self):
        self.c("scratch").drop()
        self.c("scratch").create_index("u", unique=True)
        self.c("scratch").insert_one({"u": 1})
        try:
            self.c("scratch").insert_one({"u": 1})
        except PyMongoError as e:
            return f"duplicate rejected (E11000): {str(e)[:60]}"
        finally:
            self.c("scratch").drop()
        raise RuntimeError("duplicate accepted")

    def probe_ttl_expiry(self):
        self.c("scratch").create_index("expires_at", expireAfterSeconds=0)
        return "TTL index (expireAfterSeconds; background sweep every 60 s)"

    def probe_text_index(self):
        self.op_text_search({"words": ["wireless"], "limit": 1})
        return "text index + $text (community); Atlas Search for BM25/analyzers"

    def probe_geo_index(self):
        self.op_geo_near({"lat": 52.5, "lon": 13.4, "km": 50, "limit": 1})
        return "2dsphere index + $nearSphere / $geoWithin"

    def probe_vector_index(self):
        self.op_vector_knn({"vector": [0.1] * 8, "k": 1})
        return "$vectorSearch"

    def probe_partial_update(self):
        self.c("customers").update_one({"_id": 1}, {"$set": {"probe": 1}})
        self.c("customers").update_one({"_id": 1}, {"$unset": {"probe": ""}})
        return "$set / $unset / $inc operators"

    def probe_atomic_increment(self):
        d = self.op_atomic_increment({"product_id": 1, "warehouse_id": 1})
        return f"$inc via findOneAndUpdate -> {d}"

    def probe_optimistic_concurrency(self):
        r = self.op_conditional_update({"product_id": 1, "warehouse_id": 1})
        return f"filter on version + $inc version -> {r}"

    def probe_multi_doc_transaction(self):
        with self.raw.start_session() as s:
            s.start_transaction()
            self.c("scratch").insert_one({"_id": "tx1"}, session=s)
            s.abort_transaction()
        if self.c("scratch").find_one({"_id": "tx1"}):
            raise RuntimeError("rollback did not undo the insert")
        return "multi-document ACID transactions with abort (replica set / sharded)"

    def probe_tunable_consistency(self):
        coll = self.c("scratch").with_options(write_concern=WriteConcern(w="majority", j=True), read_preference=ReadPreference.PRIMARY)
        coll.insert_one({"_id": "wc"})
        coll.delete_one({"_id": "wc"})
        return "writeConcern (w, j, wtimeout), readConcern (local/majority/linearizable), readPreference"

    def probe_array_operators(self):
        self.c("scratch").replace_one({"_id": "arr"}, {"_id": "arr", "tags": ["a"]}, upsert=True)
        self.c("scratch").update_one({"_id": "arr"}, {"$push": {"tags": "b"}})
        n = self.c("scratch").count_documents({"tags": "b"})
        self.c("scratch").delete_one({"_id": "arr"})
        return f"$push/$addToSet/$pull, $elemMatch, array equality match -> {n}"

    def probe_schema_validation(self):
        self.db.drop_collection("scratch_v")
        self.db.create_collection("scratch_v", validator={"$jsonSchema": {"bsonType": "object", "required": ["x"], "properties": {"x": {"bsonType": "int"}}}})
        try:
            self.db["scratch_v"].insert_one({"y": 1})
        except PyMongoError:
            return "$jsonSchema validator rejects non-conforming documents"
        finally:
            self.db.drop_collection("scratch_v")
        raise RuntimeError("validator accepted invalid document")

    def probe_change_streams(self):
        with self.c("scratch").watch(max_await_time_ms=500) as stream:
            self.c("scratch").insert_one({"_id": "cs"})
            ev = stream.try_next()
            t0 = time.time()
            while ev is None and time.time() - t0 < 3:
                ev = stream.try_next()
            self.c("scratch").delete_one({"_id": "cs"})
            if ev is None:
                raise RuntimeError("no change event")
            return f"change streams (oplog-backed) -> {ev.get('operationType')}"

    def probe_server_side_scripting(self):
        try:
            self.db.command("eval", "1+1")
            return "db.eval (removed in 4.2)"
        except OperationFailure as e:
            raise Unsupported(f"no server-side JS since 4.2 ({str(e)[:50]}); $function/$accumulator in aggregation only")

    def probe_pubsub(self):
        raise Unsupported("no pub/sub; change streams or capped tailable cursors")

    def probe_explain_plan(self):
        r = self.c("orders").find({"customer_id": 1}).explain()
        return f"explain(executionStats) -> {r.get('queryPlanner', {}).get('winningPlan', {}).get('stage', r.get('queryPlanner', {}).get('winningPlan'))}"[:200]

    def probe_large_document(self):
        self.c("scratch").replace_one({"_id": "big"}, {"_id": "big", "b": b"x" * (15 * 1024 * 1024)}, upsert=True)
        n = len(self.c("scratch").find_one({"_id": "big"})["b"])
        self.c("scratch").delete_one({"_id": "big"})
        return f"{n/1048576:.0f} MB document ok (BSON limit 16 MB; GridFS beyond)"

    def probe_many_fields(self):
        self.c("scratch").replace_one({"_id": "wide"}, {"_id": "wide", **{f"f{i}": i for i in range(5000)}}, upsert=True)
        n = len(self.c("scratch").find_one({"_id": "wide"}))
        self.c("scratch").delete_one({"_id": "wide"})
        return f"{n} fields"

    def probe_read_from_replica(self):
        reps = [t for t in self.cfg.targets if t.role == "replica"]
        if not reps:
            raise Unsupported("single node")
        c = self.engine.connect(reps[0])
        try:
            c.db.get_collection("customers", read_preference=ReadPreference.SECONDARY_PREFERRED).find_one({"_id": 1})
            return "readPreference=secondary/secondaryPreferred/nearest (+ readConcern)"
        finally:
            c.close()

    def probe_snapshot_backup(self):
        self.raw.admin.command("fsync", lock=True)
        self.raw.admin.command("fsyncUnlock")
        return "fsyncLock + filesystem snapshot; mongodump --oplog / mongorestore --oplogReplay (exercised in the backup phase)"

    def probe_slow_query_log(self):
        st = self.db.command("profile", -1)
        return f"profiler level {st.get('was')} slowms={st.get('slowms')} -> system.profile + JSON log 'Slow query' (getLog global)"

    def probe_runtime_log_level(self):
        cur = self.raw.admin.command("getParameter", 1, logLevel=1).get("logLevel", 0)
        self.raw.admin.command("setParameter", 1, logLevel=cur)
        return f"setParameter logLevel / logComponentVerbosity (current logLevel {cur}); db.setProfilingLevel per database"

    STATIC_PROBES = dict(STATIC_PROBES, **{
        "audit_log": ("unsupported", "auditLog is MongoDB Enterprise / Percona Server for MongoDB; Community has the JSON log + profiler only"),
        "structured_logs": ("supported", "JSON log lines since 4.4 ({t, s, c, id, ctx, msg, attr}); getLog returns the in-memory ring"),
        "incremental_backup": ("unsupported", "mongodump is always full; Percona PBM / Ops Manager / Atlas do physical + incremental"),
        "pitr": ("supported", "oplog: mongodump --oplog captures it, PBM tails it continuously (replSetResizeOplog sets the window)"),
    })


class MongoEngine(Engine):
    driver = "pymongo"

    def connect(self, target: Target, *, timeout: float = 10.0) -> MongoClientAdapter:
        auth = f"{target.user}:{target.password}@" if target.user else ""
        direct = target.extra.get("direct", True)
        uri = f"mongodb://{auth}{target.host}:{target.port}/?directConnection={'true' if direct else 'false'}"
        if target.extra.get("replica_set"):
            uri += f"&replicaSet={target.extra['replica_set']}"
        if target.extra.get("auth_source"):
            uri += f"&authSource={target.extra['auth_source']}"
        raw = MongoClient(uri, serverSelectionTimeoutMS=int(timeout * 1000), connectTimeoutMS=int(timeout * 1000),
                          socketTimeoutMS=120_000, appname="nslab")
        raw.admin.command("ping")
        return MongoClientAdapter(self, target, raw)

    def create_schema(self, client: MongoClientAdapter) -> list[str]:
        db = client.db
        idx = [
            ("customers", [("email", ASCENDING)], {"unique": True}),
            ("customers", [("country_code", ASCENDING), ("tier", ASCENDING)], {}),
            ("customers", [("location", "2dsphere")], {}),
            ("customers", [("attrs.lang", ASCENDING), ("tier", ASCENDING)], {}),
            ("categories", [("parent_id", ASCENDING)], {}),
            ("products", [("category_id", ASCENDING), ("price", DESCENDING)], {}),
            ("products", [("sku", ASCENDING)], {"unique": True}),
            ("products", [("attrs.tags", ASCENDING)], {}),
            ("products", [("description", "text")], {}),
            ("orders", [("customer_id", ASCENDING)], {}),
            ("orders", [("status", ASCENDING), ("ordered_at", ASCENDING)], {}),
            ("orders", [("ordered_at", ASCENDING), ("_id", ASCENDING)], {}),
            ("order_items", [("order_id", ASCENDING)], {}),
            ("order_items", [("product_id", ASCENDING)], {}),
            ("events", [("customer_id", ASCENDING)], {}),
            ("inventory", [("product_id", ASCENDING), ("warehouse_id", ASCENDING)], {}),
            ("scratch", [("expires_at", ASCENDING)], {"expireAfterSeconds": 0}),
        ]
        done = []
        for coll, keys, kw in idx:
            try:
                db[coll].create_index(keys, **kw)
                done.append(f"{coll}: {keys} {kw or ''}".strip())
            except PyMongoError as e:
                done.append(f"FAILED {coll} {keys}: {str(e)[:100]}")
        return done

    load_workers = 4
    load_batch = 5000
    load_method = "insert_many(5000, ordered=False, w=1)"

    def durability_levels(self) -> list[str]:
        lv = ["w0_unacknowledged", "w1", "w1_journal"]
        if len(self.cfg.targets) > 1 and self.cfg.replicas:
            lv += ["w_majority", "w_majority_journal"]
        return lv

    def read_levels(self) -> list[tuple[str, str | None]]:
        lv: list[tuple[str, str | None]] = [("primary", None), ("local", None), ("majority", None)]
        if self.cfg.replicas:
            lv.append(("secondaryPreferred", self.cfg.replicas[0].name))
            lv.append(("linearizable", None))
        return lv

    def create_index(self, client: MongoClientAdapter, name: str, coll: str, fields: list[str]) -> str:
        client.c(coll).create_index([(f, ASCENDING) for f in fields], name=name)
        return f"createIndex({coll}, {fields}) as {name}"

    def drop_index(self, client: MongoClientAdapter, name: str, coll: str) -> None:
        try:
            client.c(coll).drop_index(name)
        except OperationFailure:
            pass

    def max_connections(self, client: MongoClientAdapter) -> int | None:
        try:
            st = client.raw.admin.command("serverStatus")
            c = st.get("connections", {})
            return int(c.get("current", 0)) + int(c.get("available", 0))
        except Exception:  # noqa: BLE001
            return None

    def replication_status(self, client: MongoClientAdapter) -> dict[str, Any]:
        try:
            st = client.raw.admin.command("replSetGetStatus")
        except OperationFailure as e:
            return {"error": str(e)[:100]}
        members = []
        for m in st.get("members", []):
            members.append({"name": m.get("name"), "state": m.get("stateStr"), "optime": str(m.get("optimeDate")),
                            "lag_s": (st.get("date") - m.get("optimeDate")).total_seconds() if m.get("optimeDate") and st.get("date") else None})
        return {"set": st.get("set"), "my_state": st.get("myState"), "members": members}

    def promote(self, target: Target) -> str:
        return "automatic election (no manual promote)"

    # --- backup / restore ------------------------------------------------------------------------------
    BACKUP_STRATEGY = {
        "tool": "mongodump --oplog --gzip --archive (logical, cluster-consistent thanks to the oplog captured during the dump); physical alternative: fsyncLock + volume snapshot",
        "consistency": "point-in-time at the end of the dump (--oplogReplay applies the oplog captured while dumping)",
        "restore": "online: dropDatabase, then mongorestore --oplogReplay --drop against the primary (secondaries replicate the restore); --oplogReplay cannot be combined with namespace filters",
        "incremental": "none in Community tooling; Percona PBM does physical + incremental + selective restores",
        "pitr": "oplog-based — keep the oplog window (replSetResizeOplog) longer than the backup interval; PBM/Ops Manager tail it continuously",
        "schedule": "cron / CronJob running mongodump on a secondary (readPreference=secondary) or PBM's scheduler",
        "retention": "external: archives are self-contained files; PBM has retention policies",
    }

    def data_bytes(self, client: MongoClientAdapter) -> int | None:
        """On-disk size of the lab database (WiredTiger storageSize + indexSize); /data/db also holds the oplog + journal."""
        st = client.db.command("dbStats")
        return int(st.get("storageSize", 0) + st.get("indexSize", 0)) or None

    def _cli_port(self, target: Target) -> str:
        """Port mongod listens on inside its container (the compose command's --port; host ports are remapped)."""
        if target.extra.get("container_port"):
            return str(target.extra["container_port"])
        cmd = dockerctl.inspect(target.container).get("Config", {}).get("Cmd") or []
        return cmd[cmd.index("--port") + 1] if "--port" in cmd else "27017"

    def backup(self, client: MongoClientAdapter, tag: str, *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        if not t.container:
            raise Unsupported("no container: run mongodump from the host against the URI")
        port = self._cli_port(t)
        cmd = ["mongodump", f"--host=localhost:{port}", "--oplog", "--gzip", f"--archive=/backups/{tag}.archive.gz"]
        rc, out, err = dockerctl.exec_in(t.container, cmd, user="0", timeout=1800)
        if rc != 0:
            raise EngineError(f"mongodump rc={rc}: {err[-300:]}")
        dumped = [ln.strip() for ln in err.splitlines() if "done dumping" in ln]
        size = dockerctl.du_bytes(t.container, f"/backups/{tag}.archive.gz")
        db_stats = client.db.command("dbStats")
        return {"method": "mongodump --oplog --gzip --archive", "online": True, "artifact": f"/backups/{tag}.archive.gz", "artifact_bytes": size,
                "collections_dumped": len(dumped), "dataSize": db_stats.get("dataSize"), "storageSize": db_stats.get("storageSize"),
                "indexSize": db_stats.get("indexSize"), "data_dir_bytes": dockerctl.du_bytes(t.container, "/data/db"),
                "oplog_captured": any("writing captured oplog" in ln for ln in err.splitlines()),
                "physical_alternative": self._fsync_lock_window(client, t)}

    def _fsync_lock_window(self, client: MongoClientAdapter, t: Target) -> dict[str, Any]:
        """How long writes would be frozen for a filesystem snapshot: fsyncLock -> (cp -a of the data dir) -> fsyncUnlock."""
        t0 = time.perf_counter()
        client.raw.admin.command("fsync", lock=True)
        try:
            rc, out, err = dockerctl.exec_in(t.container, ["sh", "-c", "rm -rf /backups/fs-copy && cp -a /data/db /backups/fs-copy && du -sb /backups/fs-copy | cut -f1 && rm -rf /backups/fs-copy"], user="0", timeout=1800)
        finally:
            client.raw.admin.command("fsyncUnlock")
        return {"method": "fsyncLock + cp -a /data/db + fsyncUnlock (stand-in for a volume snapshot)", "writes_blocked_ms": round((time.perf_counter() - t0) * 1000, 1),
                "bytes": int(out.strip()) if rc == 0 and out.strip().isdigit() else None}

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        return [(self.cfg.primary.container, f"/backups/{tag}.archive.gz")]

    def restore(self, client: MongoClientAdapter, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        t = self.cfg.primary
        port = self._cli_port(t)
        db_name = t.database or "lab"
        client.raw.drop_database(db_name)
        # --oplogReplay refuses namespace filters, so the whole archive (lab + admin.system.version) is replayed
        cmd = ["mongorestore", f"--host=localhost:{port}", "--gzip", f"--archive=/backups/{tag}.archive.gz", "--oplogReplay", "--drop"]
        rc, out, err = dockerctl.exec_in(t.container, cmd, user="0", timeout=3600)
        if rc != 0:
            raise EngineError(f"mongorestore rc={rc}: {err[-300:]}")
        summary = next((ln.strip() for ln in reversed(err.splitlines()) if "document(s) restored successfully" in ln), "")
        return {"method": "dropDatabase + mongorestore --oplogReplay --drop", "online": True, "summary": summary[:200],
                "index_builds": sum(1 for ln in err.splitlines() if "index" in ln.lower() and "building" in ln.lower())}

    # --- logging -----------------------------------------------------------------------------------------
    LOGGING_STRATEGY = {
        "server_log": "JSON lines on stdout ({t, s, c, id, ctx, msg, attr}); systemLog.path + logRotate for files; getLog reads the in-memory ring",
        "slow_query": "operationProfiling: mode slowOp/all, slowOpThresholdMs, slowOpSampleRate -> capped system.profile per database + 'Slow query' log lines (durationMillis, planSummary, keysExamined/docsExamined)",
        "audit": "auditLog is Enterprise / Percona PSMDB (destination file|syslog|console, filter) — not in Community",
        "runtime": "setParameter logLevel / logComponentVerbosity, db.setProfilingLevel(level, {slowms, sampleRate}) — no restart",
    }

    def server_log_facts(self, client: MongoClientAdapter) -> dict[str, Any]:
        lg = client.raw.admin.command("getLog", "global")
        lines = lg.get("log", [])
        return {"format": "json", "where": "stdout (docker logs) — systemLog.path when set; getLog: global = last 1024 lines in memory",
                "ring_lines": lg.get("totalLinesWritten"), "sample": [ln[:200] for ln in lines[-3:]]}

    def slow_query_setup(self, client: MongoClientAdapter, threshold_ms: int) -> str:
        client.db.command("profile", 0)
        client.db.command("profile", 1, slowms=threshold_ms, sampleRate=1.0)
        return f"db.setProfilingLevel(1, {{slowms: {threshold_ms}, sampleRate: 1.0}}) on '{client.db.name}' (system.profile capped 1 MB + 'Slow query' JSON log lines)"

    def slow_query_fast_op(self, client: MongoClientAdapter) -> str:
        client.c("customers").find_one({"_id": 1}, {"name": 1})
        return "find customers {_id: 1}"

    def slow_query_slow_op(self, client: MongoClientAdapter) -> str:
        list(client.c("events").aggregate([{"$group": {"_id": "$event_type", "n": {"$sum": 1}}}]))
        return "aggregate events [$group by event_type] (1M-doc collection scan)"

    def slow_query_entries(self, client: MongoClientAdapter, threshold_ms: int) -> list[dict[str, Any]]:
        out = []
        for d in client.db["system.profile"].find({"millis": {"$gte": threshold_ms}}).sort("ts", -1).limit(10):
            cmd = d.get("command", {})
            text = f"{d.get('op')} {d.get('ns')} " + json.dumps({k: v for k, v in cmd.items() if k not in ("lsid", "$clusterTime", "$db", "$readPreference")}, default=str)[:160]
            out.append({"ms": d.get("millis"), "text": text, "planSummary": d.get("planSummary"), "docsExamined": d.get("docsExamined"), "keysExamined": d.get("keysExamined")})
        slow_lines = [ln for ln in client.raw.admin.command("getLog", "global").get("log", []) if '"msg":"Slow query"' in ln]
        if slow_lines:
            try:
                j = json.loads(slow_lines[-1])
                attr = j.get("attr", {})
                out.append({"ms": attr.get("durationMillis"), "text": f"[JSON log] Slow query ns={attr.get('ns')} planSummary={attr.get('planSummary')} ({len(slow_lines)} 'Slow query' lines in getLog)", "kind": "server-log"})
            except json.JSONDecodeError:
                pass
        return out

    def slow_query_teardown(self, client: MongoClientAdapter) -> None:
        client.db.command("profile", 1, slowms=100, sampleRate=1.0)

    def audit_log_setup(self, client: MongoClientAdapter) -> str:
        raise Unsupported("auditLog is MongoDB Enterprise / Percona Server for MongoDB (auditLog.destination file|syslog|console + filter); Community has the profiler + JSON log")

    def log_level_runtime(self, client: MongoClientAdapter) -> str:
        adm = client.raw.admin
        before = adm.command("getParameter", 1, logComponentVerbosity=1)["logComponentVerbosity"]["command"]["verbosity"]
        adm.command("setParameter", 1, logComponentVerbosity={"command": {"verbosity": 1}})
        after = adm.command("getParameter", 1, logComponentVerbosity=1)["logComponentVerbosity"]["command"]["verbosity"]
        adm.command("setParameter", 1, logComponentVerbosity={"command": {"verbosity": before}})
        if after != 1:
            raise EngineError(f"verbosity did not change (got {after})")
        return f"setParameter logComponentVerbosity.command {before} -> 1 -> {before} (no restart; every command logged at 1)"
