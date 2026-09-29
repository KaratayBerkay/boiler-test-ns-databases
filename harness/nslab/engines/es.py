"""HTTP search adapter: Elasticsearch 8 / OpenSearch 2 — via httpx (no vendor client, just the REST API).

One index per collection (customers, categories, products, orders, events, inventory), _id = the dataset id.
`products.description` is a `text` field (BM25), `products.embedding` a `dense_vector` (kNN), `customers.location`
a `geo_point`. Bulk load uses the `_bulk` NDJSON endpoint. The engine is eventually consistent for search
(refresh interval), so the harness refreshes after load and the replication phase measures refresh visibility.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import httpx

from ..config import Target
from ..datagen import read_docs
from ..ops import Unsupported
from .. import dockerctl
from .base import Client, Engine, EngineError, batched, iso

VEC_DIM = 8


def _doc(coll: str, d: dict) -> dict:
    d = dict(d)
    if coll == "customers" and d.get("location"):
        d["location"] = {"lat": d["location"]["lat"], "lon": d["location"]["lon"]}
    if coll == "inventory":
        d["_key"] = f"{d['product_id']}:{d['warehouse_id']}"
    return d


def _id(coll: str, d: dict) -> str:
    if coll == "inventory":
        return f"{d['product_id']}:{d['warehouse_id']}"
    return str(d["id"])


class EsClient(Client):
    def __init__(self, engine: "EsEngine", target: Target, http: httpx.Client):
        super().__init__(engine, target, http)

    def close(self) -> None:
        try:
            self.raw.close()
        except Exception:  # noqa: BLE001
            pass

    def _req(self, method: str, path: str, body: Any = None, ndjson: str | None = None, params: dict | None = None):
        headers = {}
        if ndjson is not None:
            headers["Content-Type"] = "application/x-ndjson"
            content = ndjson
        elif body is not None:
            headers["Content-Type"] = "application/json"
            content = json.dumps(body, default=iso)
        else:
            content = None
        r = self.raw.request(method, path, content=content, headers=headers, params=params)
        if r.status_code >= 400:
            raise EngineError(f"{method} {path} -> {r.status_code}: {r.text[:300]}")
        return r.json() if r.content else {}

    def idx(self, coll: str) -> str:
        return f"{self.engine.prefix}{coll}"

    # --- lifecycle -----------------------------------------------------------------------------
    def ping(self):
        return self._req("GET", "/")

    def server_version(self) -> str:
        try:
            info = self._req("GET", "/")
            v = info.get("version", {})
            dist = v.get("distribution", "elasticsearch")
            return f"{dist} {v.get('number')}"
        except Exception as e:  # noqa: BLE001
            return f"unknown ({e})"

    def role(self) -> str:
        """'master' for the elected master, else 'node' (every node coordinates reads and writes)."""
        try:
            local = self._req("GET", "/_nodes/_local", params={"filter_path": "nodes.*.name"})
            me = next(iter(local.get("nodes", {}).values()), {}).get("name")
            master = self._req("GET", "/_cat/master", params={"format": "json"})
            if me and master and master[0].get("node") == me:
                return "master"
            return "node" if me else self.target.role
        except Exception:  # noqa: BLE001
            return self.target.role

    # --- primitives --------------------------------------------------------------------------------
    def count(self, coll: str) -> int:
        self.refresh(coll)          # _count is post-refresh; make it reflect just-written docs (load check / repl catch-up)
        return int(self._req("GET", f"/{self.idx(coll)}/_count").get("count", 0))

    def get(self, coll: str, key: Any) -> dict | None:
        try:
            r = self._req("GET", f"/{self.idx(coll)}/_doc/{key}")
            return r.get("_source")
        except EngineError:
            return None

    def put(self, coll: str, doc: dict) -> None:
        self._req("PUT", f"/{self.idx(coll)}/_doc/{_id(coll, doc)}", _doc(coll, doc))

    def delete(self, coll: str, key: Any) -> None:
        try:
            self._req("DELETE", f"/{self.idx(coll)}/_doc/{key}")
        except EngineError:
            pass

    def bulk_put(self, coll: str, docs: list[dict]) -> int:
        lines = []
        idx = self.idx(coll)
        for d in docs:
            lines.append(json.dumps({"index": {"_index": idx, "_id": _id(coll, d)}}))
            lines.append(json.dumps(_doc(coll, d), default=iso))
        r = self._req("POST", "/_bulk", ndjson="\n".join(lines) + "\n", params={"refresh": "false"})
        if r.get("errors"):
            first = next((it for it in r["items"] if list(it.values())[0].get("error")), None)
            raise EngineError(f"_bulk errors: {json.dumps(first)[:300]}")
        return len(docs)

    def delete_many(self, coll: str, keys: list[Any]) -> None:
        lines = []
        idx = self.idx(coll)
        for k in keys:
            lines.append(json.dumps({"delete": {"_index": idx, "_id": str(k)}}))
        if lines:
            self._req("POST", "/_bulk", ndjson="\n".join(lines) + "\n")

    def drop_collection(self, coll: str) -> None:
        try:
            self._req("DELETE", f"/{self.idx(coll)}")
        except EngineError:
            pass

    def refresh(self, coll: str | None = None) -> None:
        self._req("POST", f"/{self.idx(coll) if coll else '_all'}/_refresh")

    def _search(self, coll: str, body: dict) -> list[dict]:
        r = self._req("POST", f"/{self.idx(coll)}/_search", body)
        return r.get("hits", {}).get("hits", [])

    # --- operations -------------------------------------------------------------------------------------
    def op_get_by_key(self, p):
        s = self.get("customers", p["id"])
        return [(s["id"], s["name"], s["email"], s["country_code"])] if s else []

    def op_multi_get(self, p):
        r = self._req("POST", f"/{self.idx('customers')}/_mget", {"ids": [str(i) for i in p["ids"]]})
        return sorted((d["_source"]["id"], d["_source"]["tier"]) for d in r["docs"] if d.get("found"))

    def op_insert_one(self, p):
        self._req("PUT", f"/{self.idx('events')}/_doc/{p['id']}", _doc("events", p))
        return None

    def op_update_field(self, p):
        self._req("POST", f"/{self.idx('customers')}/_update/{p['id']}", {"doc": {"last_login": p["value"]}})
        return None

    def op_upsert(self, p):
        k = f"{p['product_id']}:{p['warehouse_id']}"
        self._req("PUT", f"/{self.idx('inventory')}/_doc/{k}", {"product_id": p["product_id"], "warehouse_id": p["warehouse_id"],
                                                                "qty": p["qty"], "updated_at": iso(__import__("datetime").datetime.utcnow()), "version": 1})
        return None

    def op_delete_one(self, p):
        self._req("DELETE", f"/{self.idx('events')}/_doc/{p['id']}")
        return None

    def op_atomic_increment(self, p):
        k = f"{p['product_id']}:{p['warehouse_id']}"
        r = self._req("POST", f"/{self.idx('inventory')}/_update/{k}",
                      {"script": {"source": "ctx._source.qty += 1", "lang": "painless"}, "_source": True}, params={"_source": "qty"})
        # _update does not return the new value by default; fetch it
        return [(self.get("inventory", k)["qty"],)]

    def op_conditional_update(self, p):
        k = f"{p['product_id']}:{p['warehouse_id']}"
        src = self.get("inventory", k)
        if not src:
            return [(0, -1)]
        seq = self._req("GET", f"/{self.idx('inventory')}/_doc/{k}")
        try:
            self._req("POST", f"/{self.idx('inventory')}/_update/{k}",
                      {"doc": {"qty": 250, "version": src["version"] + 1}},
                      params={"if_seq_no": seq["_seq_no"], "if_primary_term": seq["_primary_term"]})
            return [(1, src["version"] + 1)]
        except EngineError:
            return [(0, src["version"])]

    def op_batch_insert_1000(self, p):
        self.bulk_put("events", p["docs"])
        return None

    def op_range_by_secondary(self, p):
        hits = self._search("orders", {"size": 200, "query": {"term": {"customer_id": p["customer_id"]}}, "sort": [{"id": "asc"}],
                                       "_source": ["id", "status", "total_amount"]})
        return [(h["_source"]["id"], h["_source"]["status"], h["_source"]["total_amount"]) for h in hits]

    def op_range_by_time(self, p):
        hits = self._search("events", {"size": p["limit"], "query": {"bool": {"filter": [
            {"term": {"event_type": p["event_type"]}}, {"range": {"occurred_at": {"gte": iso(p["start"]), "lt": iso(p["end"])}}}]}},
            "sort": [{"occurred_at": "asc"}, {"id": "asc"}], "_source": ["id", "customer_id", "occurred_at"]})
        return [(h["_source"]["id"], h["_source"]["customer_id"], h["_source"]["occurred_at"]) for h in hits]

    def op_filter_nested(self, p):
        r = self._req("POST", f"/{self.idx('customers')}/_count", {"query": {"bool": {"filter": [
            {"term": {"attrs.lang": "tr"}}, {"term": {"tier": "pro"}}]}}})
        return [(r["count"],)]

    def op_array_contains(self, p):
        r = self._req("POST", f"/{self.idx('products')}/_count", {"query": {"bool": {"filter": [
            {"term": {"attrs.tags": "eco"}}, {"term": {"active": True}}]}}})
        return [(r["count"],)]

    def op_top_n_sorted(self, p):
        hits = self._search("products", {"size": p["n"], "query": {"bool": {"filter": [
            {"term": {"category_id": p["category_id"]}}, {"term": {"active": True}}]}},
            "sort": [{"price": "desc"}, {"id": "asc"}], "_source": ["id", "price"]})
        return [(h["_source"]["id"], h["_source"]["price"]) for h in hits]

    def op_pagination_offset(self, p):
        hits = self._search("orders", {"from": p["offset"], "size": p["page"], "query": {"match_all": {}},
                                       "sort": [{"ordered_at": "asc"}, {"id": "asc"}], "_source": ["id", "ordered_at"]})
        return [(h["_source"]["id"], h["_source"]["ordered_at"]) for h in hits]

    def op_pagination_keyset(self, p):
        cur = getattr(self, "_cursor", None)
        if cur is None:
            hits = self._search("orders", {"from": p["offset"] - 1, "size": 1, "query": {"match_all": {}},
                                           "sort": [{"ordered_at": "asc"}, {"id": "asc"}], "_source": ["id", "ordered_at"]})
            cur = self._cursor = hits[0]["sort"]
        hits = self._search("orders", {"size": p["page"], "query": {"match_all": {}}, "search_after": cur,
                                       "sort": [{"ordered_at": "asc"}, {"id": "asc"}], "_source": ["id", "ordered_at"]})
        return [(h["_source"]["id"], h["_source"]["ordered_at"]) for h in hits]

    def op_count_filtered(self, p):
        r = self._req("POST", f"/{self.idx('orders')}/_count", {"query": {"term": {"status": "pending"}}})
        return [(r["count"],)]

    def op_aggregate_group(self, p):
        r = self._req("POST", f"/{self.idx('orders')}/_search", {"size": 0, "aggs": {"by_country": {
            "terms": {"field": "shipping_country", "size": 50, "order": {"_key": "asc"}},
            "aggs": {"total": {"sum": {"field": "total_amount"}}}}}})
        buckets = r["aggregations"]["by_country"]["buckets"]
        return [(b["key"], b["doc_count"], round(b["total"]["value"], 2)) for b in buckets]

    def op_aggregate_time_bucket(self, p):
        r = self._req("POST", f"/{self.idx('events')}/_search", {"size": 0, "query": {"term": {"event_type": "checkout"}},
            "aggs": {"m": {"date_histogram": {"field": "occurred_at", "calendar_interval": "month", "format": "yyyy-MM"},
                           "aggs": {"total": {"sum": {"field": "value_num"}}}}}})
        buckets = r["aggregations"]["m"]["buckets"]
        return [(b["key_as_string"], b["doc_count"], round(b["total"]["value"], 4)) for b in buckets if b["doc_count"]]

    def op_join_lookup(self, p):
        name = (self.get("customers", p["customer_id"]) or {}).get("name")
        hits = self._search("orders", {"size": 200, "query": {"term": {"customer_id": p["customer_id"]}}, "sort": [{"id": "asc"}],
                                       "_source": ["id", "total_amount"]})
        return [(h["_source"]["id"], name, h["_source"]["total_amount"]) for h in hits]

    def op_graph_traversal(self, p):
        prod = self.get("products", p["product_id"])
        cat = prod["category_id"] if prod else None
        path = []
        while cat is not None and len(path) < 10:
            c = self.get("categories", cat)
            if not c:
                break
            path.append((c["depth"], c["id"]))
            cat = c.get("parent_id")
        return sorted(path)

    def op_text_search(self, p):
        hits = self._search("products", {"size": p["limit"], "query": {"bool": {"must": [
            {"match": {"description": {"query": w, "operator": "and"}}} for w in p["words"]]}}, "_source": False,
            "sort": [{"id": "asc"}]})
        return sorted((int(h["_id"]),) for h in hits)

    def op_geo_near(self, p):
        hits = self._search("customers", {"size": p["limit"], "query": {"bool": {"filter": {"geo_distance": {
            "distance": f"{p['km']}km", "location": {"lat": p["lat"], "lon": p["lon"]}}}}},
            "sort": [{"_geo_distance": {"location": {"lat": p["lat"], "lon": p["lon"]}, "order": "asc", "unit": "km"}}], "_source": ["id"]})
        return [(h["_source"]["id"],) for h in hits]

    def op_vector_knn(self, p):
        r = self._req("POST", f"/{self.idx('products')}/_search", {"knn": {"field": "embedding", "query_vector": p["vector"],
                                                                           "k": p["k"], "num_candidates": 100}, "_source": ["id"], "size": p["k"]})
        return [(h["_source"]["id"],) for h in r["hits"]["hits"]]

    def op_transaction_transfer(self, p):
        raise Unsupported("no multi-document transactions (single-document atomicity only)")

    def op_ttl_set(self, p):
        # ES has no per-doc TTL since 5.x; model it with an expiry timestamp field + ILM/delete-by-query sweep
        self._req("PUT", f"/{self.idx('events')}/_doc/ttl-{p['key']}",
                  {"id": -1, "note": "ttl", "expires_at": iso(__import__("datetime").datetime.utcnow() + __import__("datetime").timedelta(seconds=p["seconds"]))})
        return None

    def op_scan_full(self, p):
        return [(self.count("events"),)]

    # --- optimisation hooks -----------------------------------------------------------------------------
    def opt_fetch_full(self, coll, key):
        s = self.get(coll, key)
        return [(s["id"], len(s))] if s else []

    def opt_fetch_projected(self, coll, key, fields):
        r = self._req("GET", f"/{self.idx(coll)}/_doc/{key}", params={"_source_includes": ",".join(fields)})
        s = r.get("_source", {})
        return [tuple(s.get(f) for f in fields)]

    def opt_order_embedded(self, order_id):
        s = self.get("orders", order_id)
        return [(s["id"], len(s.get("items", [])))] if s else []

    def opt_order_referenced(self, order_id):
        s = self._req("GET", f"/{self.idx('orders')}/_doc/{order_id}", params={"_source_includes": "id,status,total_amount"})["_source"]
        hits = self._search("order_items", {"size": 50, "query": {"term": {"order_id": order_id}}, "_source": ["id"]})
        return [(s["id"], len(hits))]

    def opt_write_durable(self, level, doc):
        # ES exposes durability via wait_for_active_shards and index.translog.durability; model per-request refresh
        params = {}
        if level == "refresh_true":
            params["refresh"] = "true"
        elif level == "wait_for":
            params["refresh"] = "wait_for"
        self._req("PUT", f"/{self.idx('events')}/_doc/{doc['id']}", _doc("events", doc), params=params)
        return None

    def opt_read_level(self, level, key):
        s = self.get("customers", key)
        return [(s["id"], s["name"])] if s else []

    # --- probes -----------------------------------------------------------------------------------------------
    STATIC_PROBES = {
        "secondary_index": ("supported", "every field is indexed by default (inverted index / BKD tree / doc_values)"),
        "compound_index": ("supported", "bool query combines per-field indexes; no explicit composite index needed"),
        "text_index": ("supported", "analyzed text fields with BM25 (the core feature)"),
        "geo_index": ("supported", "geo_point / geo_shape (BKD tree) + geo_distance / geo_bounding_box"),
        "vector_index": ("supported", "dense_vector with HNSW + kNN search"),
        "aggregation": ("supported", "aggregations framework (terms, date_histogram, metrics, pipeline)"),
        "nested_query": ("supported", "object/nested field types + nested query"),
        "bulk_import": ("supported", "_bulk NDJSON API"),
        "server_side_join": ("unsupported", "no server-side joins across indices (denormalise, parent/join field within one index, or terms-lookup)"),
        "graph_traversal": ("unsupported", "no graph traversal (client-side, or X-Pack Graph explore API)"),
        "pubsub": ("unsupported", "no pub/sub"),
        "multi_doc_transaction": ("unsupported", "single-document atomicity only; no multi-document transactions"),
    }

    def probe_unique_constraint(self):
        raise Unsupported("no unique constraint; _id uniqueness only (op_type=create rejects a duplicate _id)")

    def probe_ttl_expiry(self):
        raise Unsupported("per-document _ttl removed in 5.0; use an expiry timestamp + ILM rollover/delete or delete-by-query")

    def probe_partial_update(self):
        self._req("POST", f"/{self.idx('customers')}/_update/1", {"doc": {"probe": 1}})
        return "_update with a partial doc or painless script (reindexes the document internally)"

    def probe_atomic_increment(self):
        return f"_update painless script ctx._source.qty += 1 -> {self.op_atomic_increment({'product_id':1,'warehouse_id':1})}"

    def probe_optimistic_concurrency(self):
        return f"if_seq_no + if_primary_term (or external version) -> {self.op_conditional_update({'product_id':1,'warehouse_id':1})}"

    def probe_tunable_consistency(self):
        self._req("PUT", f"/{self.idx('scratch') if False else self.idx('events')}/_doc/wc-probe", {"id": -2}, params={"wait_for_active_shards": "all"})
        self.delete("events", "wc-probe")
        return "wait_for_active_shards per write; refresh=true|wait_for|false for read-your-write visibility"

    def probe_array_operators(self):
        r = self._req("POST", f"/{self.idx('products')}/_count", {"query": {"term": {"attrs.tags": "eco"}}})
        return f"arrays are first-class (a field is multi-valued); term query matches any element -> {r['count']} match 'eco'"

    def probe_schema_validation(self):
        return "explicit mappings + dynamic:strict rejects unknown fields; field data types enforced at index time"

    def probe_change_streams(self):
        raise Unsupported("no change feed; poll with a sorted timestamp/_seq_no or use Logstash/change-detection")

    def probe_server_side_scripting(self):
        r = self._req("POST", "/_scripts/painless/_execute", {"script": {"source": "1 + 1"}})
        return f"Painless scripts (ingest pipelines, script queries, script fields, _update) -> {r.get('result')}"

    def probe_explain_plan(self):
        r = self._req("GET", f"/{self.idx('orders')}/_validate/query", {"query": {"term": {"status": "pending"}}}, params={"explain": "true"})
        prof = self._req("POST", f"/{self.idx('orders')}/_search", {"profile": True, "size": 0, "query": {"term": {"status": "pending"}}})
        took = prof.get("took")
        return f"_validate/query?explain and Search Profile API (per-shard query/collector timings, took={took}ms)"

    def probe_large_document(self):
        blob = "x" * (5 * 1024 * 1024)      # http.max_content_length default 100mb; 5 MB _source is already large for a search doc
        self._req("PUT", f"/{self.idx('events')}/_doc/big", {"id": -3, "blob": blob}, params={"refresh": "false"})
        s = self.get("events", "big")
        self.delete("events", "big")
        return f"{len(s['blob'])/1048576:.0f} MB _source stored (http.max_content_length 100 MB; large text hurts search)"

    def probe_many_fields(self):
        raise Unsupported("index.mapping.total_fields.limit default 1000; 5000 distinct fields triggers mapping explosion (use flattened type)")

    def probe_read_from_replica(self):
        return "reads are served by any shard copy (primary or replica) automatically; ?preference= pins routing"

    def probe_snapshot_backup(self):
        self.engine.ensure_repo(self)
        return "snapshot API into an fs repository under path.repo (S3/GCS/Azure via repository plugins); SLM schedules + retention (exercised in the backup phase)"

    def probe_slow_query_log(self):
        self._req("PUT", f"/{self.idx('customers')}/_settings", {"index.search.slowlog.threshold.query.warn": "1s"})
        self._req("PUT", f"/{self.idx('customers')}/_settings", {"index.search.slowlog.threshold.query.warn": None})
        return "per-index search/indexing slowlog thresholds (index.search.slowlog.threshold.query.warn ...), dynamic, JSON lines with took_millis + source"

    def probe_runtime_log_level(self):
        self._req("PUT", "/_cluster/settings", {"transient": {"logger.org.elasticsearch.snapshots": "DEBUG"}})
        self._req("PUT", "/_cluster/settings", {"transient": {"logger.org.elasticsearch.snapshots": None}})
        return "PUT _cluster/settings logger.<package>: DEBUG (transient/persistent), no restart"

    STATIC_PROBES = dict(STATIC_PROBES, **{
        "audit_log": ("unsupported", "xpack.security.audit.enabled needs security enabled (off in this lab); JSON audit of auth/access events"),
        "structured_logs": ("supported", "ECS JSON on stdout in the docker image (server, deprecation, slowlog datasets)"),
        "incremental_backup": ("supported", "repository stores each Lucene segment once: every snapshot after the first only uploads new segments"),
        "pitr": ("unsupported", "no translog replay between snapshots — the RPO is the SLM schedule"),
    })


class EsEngine(Engine):
    driver = "es"
    load_workers = 4
    load_batch = 5000

    def apply_features(self, features: dict) -> None:
        self.prefix = features.get("index_prefix", "lab_")
        self.distribution = features.get("distribution", "elasticsearch")

    def connect(self, target: Target, *, timeout: float = 10.0) -> EsClient:
        scheme = "https" if target.extra.get("tls") else "http"
        auth = (target.user, target.password) if target.user else None
        http = httpx.Client(base_url=f"{scheme}://{target.host}:{target.port}", auth=auth, timeout=httpx.Timeout(120, connect=timeout),
                            verify=False, limits=httpx.Limits(max_connections=100, max_keepalive_connections=50))
        c = EsClient(self, target, http)
        c.ping()
        return c

    # --- backup / restore ------------------------------------------------------------------------------
    REPO = "nslab"
    BACKUP_STRATEGY = {
        "tool": "snapshot API into a registered repository (type fs under path.repo; S3/GCS/Azure repositories in production) — segment-level incremental",
        "consistency": "per-index point-in-time at snapshot start (immutable Lucene segments; the translog is not included)",
        "restore": "online: delete (or close) the indices, POST _snapshot/<repo>/<snap>/_restore (rename_pattern to restore beside the live index); the fs repository must be the same shared path on every node",
        "incremental": "built in — a repository stores each segment once, the 2nd snapshot uploads only new segments (snapshot.stats.incremental)",
        "pitr": "no; the SLM schedule is the RPO (searchable snapshots / CCR for other goals)",
        "schedule": "SLM policy (cron schedule + retention) executed by the cluster itself — created by the backup phase",
        "retention": "SLM retention (expire_after, min_count, max_count) + DELETE _snapshot/<repo>/<snap>",
    }

    def ensure_repo(self, client: EsClient) -> None:
        client._req("PUT", f"/_snapshot/{self.REPO}", {"type": "fs", "settings": {"location": f"/backups/{self.REPO}", "compress": True}})

    def data_bytes(self, client: EsClient) -> int | None:
        """Store size of the lab indices (segments); the data dir also holds translogs and cluster state."""
        st = client._req("GET", f"/{self.prefix}*/_stats/store")
        return int((st.get("_all", {}).get("primaries", {}).get("store", {}) or {}).get("size_in_bytes", 0)) or None

    def backup(self, client: EsClient, tag: str, *, log=print) -> dict[str, Any]:
        self.ensure_repo(client)
        r = client._req("PUT", f"/_snapshot/{self.REPO}/{tag}", {"indices": f"{self.prefix}*", "include_global_state": False}, params={"wait_for_completion": "true"})
        snap = r.get("snapshot", {})
        if snap.get("state") != "SUCCESS":
            raise EngineError(f"snapshot state {snap.get('state')}: {str(snap.get('failures'))[:200]}")
        st = client._req("GET", f"/_snapshot/{self.REPO}/{tag}/_status").get("snapshots", [{}])[0].get("stats", {})
        policy = {"schedule": "0 30 1 * * ?", "name": "<nslab-{now/d}>", "repository": self.REPO,
                  "config": {"indices": [f"{self.prefix}*"], "include_global_state": False},
                  "retention": {"expire_after": "7d", "min_count": 3, "max_count": 30}}
        slm = "n/a"
        try:
            client._req("PUT", "/_slm/policy/nslab-nightly", policy)
            slm = "SLM policy nslab-nightly: daily 01:30, keep 3-30 snapshots / 7 days"
        except EngineError as e:
            slm = f"SLM not available: {str(e)[:80]}"
        t = self.cfg.primary
        repo_bytes = dockerctl.du_bytes(t.container, f"/backups/{self.REPO}") if t.container else None
        # the repository is cumulative (segments shared by every snapshot); the artifact is this snapshot's own size
        return {"method": f"PUT _snapshot/{self.REPO}/<tag> (fs repository, compress)", "online": True, "artifact": f"/backups/{self.REPO} (repository) snapshot {tag}",
                "artifact_bytes": st.get("total", {}).get("size_in_bytes"), "repository_bytes": repo_bytes, "files": st.get("total", {}).get("file_count"),
                "data_dir_bytes": dockerctl.du_bytes(t.container, "/usr/share/elasticsearch/data") if t.container else None,
                "indices": len(snap.get("indices", [])), "shards": snap.get("shards"), "server_ms": snap.get("duration_in_millis"), "slm": slm}

    def artifact_paths(self, tag: str) -> list[tuple[str, str]]:
        return [(self.cfg.primary.container, f"/backups/{self.REPO}")]      # the whole repository: snapshots share segment files

    def restore(self, client: EsClient, tag: str, info: dict[str, Any], *, log=print) -> dict[str, Any]:
        self.ensure_repo(client)                  # fresh node after down -v: re-register the repository over the copied-back files
        # action.destructive_requires_name (default since 8.0) rejects wildcard deletes: name the indices explicitly
        names = [i["index"] for i in client._req("GET", f"/_cat/indices/{self.prefix}*", params={"format": "json"})]
        if names:
            client._req("DELETE", "/" + ",".join(names))
        r = client._req("POST", f"/_snapshot/{self.REPO}/{tag}/_restore", {"indices": f"{self.prefix}*", "include_global_state": False}, params={"wait_for_completion": "true"})
        client._req("GET", "/_cluster/health", params={"wait_for_status": "yellow", "timeout": "300s"})
        sh = (r.get("snapshot") or {}).get("shards", {})
        return {"method": f"DELETE the {len(names)} {self.prefix}* indices (by name) + POST _snapshot/{self.REPO}/<tag>/_restore", "online": True, "shards": sh,
                "indices": (r.get("snapshot") or {}).get("indices")}

    def backup_incremental(self, client: EsClient, tag: str, base: dict[str, Any], *, log=print) -> dict[str, Any]:
        r = client._req("PUT", f"/_snapshot/{self.REPO}/{tag}", {"indices": f"{self.prefix}*", "include_global_state": False}, params={"wait_for_completion": "true"})
        st = client._req("GET", f"/_snapshot/{self.REPO}/{tag}/_status").get("snapshots", [{}])[0].get("stats", {})
        return {"method": "second snapshot into the same repository (only new segments are copied)",
                "artifact_bytes_delta": st.get("incremental", {}).get("size_in_bytes"), "files_delta": st.get("incremental", {}).get("file_count"),
                "total_bytes": st.get("total", {}).get("size_in_bytes"), "server_ms": (r.get("snapshot") or {}).get("duration_in_millis")}

    # --- logging -----------------------------------------------------------------------------------------
    LOGGING_STRATEGY = {
        "server_log": "ECS JSON on stdout (docker image log4j2): event.dataset elasticsearch.server / deprecation / index_search_slowlog / index_indexing_slowlog",
        "slow_query": "per-index slowlog thresholds (index.search.slowlog.threshold.{query,fetch}.{warn,info,debug,trace}, index.indexing.slowlog.threshold.index.*), dynamic; lines carry took_millis, source, total_hits",
        "audit": "xpack.security.audit.enabled (JSON audit of authentication/authorization/run_as) — needs security on; off in this lab",
        "runtime": "PUT _cluster/settings {transient|persistent: {logger.<package>: DEBUG}} — no restart",
    }

    def server_log_facts(self, client: EsClient) -> dict[str, Any]:
        t = self.cfg.primary
        out = {"format": "json (ECS)", "where": "stdout (docker logs); logs/ dir when running from the tarball"}
        if t.container:
            out["sample"] = [ln[:200] for ln in dockerctl.logs(t.container, tail=2).splitlines()]
        return out

    def slow_query_setup(self, client: EsClient, threshold_ms: int) -> str:
        self._slow_since = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() - 2))
        client._req("PUT", f"/{self.prefix}*/_settings", {"index.search.slowlog.threshold.query.warn": f"{threshold_ms}ms",
                                                          "index.search.slowlog.threshold.fetch.warn": f"{threshold_ms}ms",
                                                          "index.indexing.slowlog.threshold.index.warn": f"{threshold_ms}ms"})
        return f"PUT {self.prefix}*/_settings index.search.slowlog.threshold.query.warn={threshold_ms}ms (+fetch, indexing) — dynamic, per index"

    def slow_query_fast_op(self, client: EsClient) -> str:
        client._req("GET", f"/{client.idx('customers')}/_doc/1")
        return "GET lab_customers/_doc/1"

    def slow_query_slow_op(self, client: EsClient) -> str:
        # guard the field reads: docs written by other phases (probes, load test) may lack value_num / customer_id
        body = {"size": 1, "query": {"script_score": {"query": {"match_all": {}}, "script": {"source":
                "(doc['value_num'].size() == 0 ? 0.0 : Math.log(2 + doc['value_num'].value)) * (1 + (doc['customer_id'].size() == 0 ? 0 : doc['customer_id'].value % 7))"}}}}
        client._req("POST", f"/{client.idx('events')}/_search", body)
        return "script_score over lab_events (painless scoring of 1M docs)"

    def slow_query_entries(self, client: EsClient, threshold_ms: int) -> list[dict[str, Any]]:
        containers = [t.container for t in self.cfg.targets if t.container]
        if not containers:
            raise Unsupported("slowlog lines are on stdout — needs docker logs / a log shipper")
        out = []
        # the slowlog is written by the data node that executed the shard query, which may not be the coordinating node
        lines = [ln for c in containers for ln in dockerctl.logs(c, since=getattr(self, "_slow_since", None)).splitlines()]
        for ln in lines:
            if "slowlog" not in ln:
                continue
            try:
                j = json.loads(ln)
            except json.JSONDecodeError:
                continue
            took = j.get("elasticsearch.slowlog.took_millis")
            try:
                took = float(took)
            except (TypeError, ValueError):
                continue
            if took >= threshold_ms:
                out.append({"ms": took, "text": f"{j.get('log.logger')} {j.get('elasticsearch.slowlog.message')} {str(j.get('elasticsearch.slowlog.source'))[:140]}",
                            "logger": j.get("log.logger"), "hits": j.get("elasticsearch.slowlog.total_hits")})
        return out

    def slow_query_teardown(self, client: EsClient) -> None:
        client._req("PUT", f"/{self.prefix}*/_settings", {"index.search.slowlog.threshold.query.warn": None,
                                                          "index.search.slowlog.threshold.fetch.warn": None,
                                                          "index.indexing.slowlog.threshold.index.warn": None})

    def audit_log_setup(self, client: EsClient) -> str:
        raise Unsupported("xpack.security.audit.enabled needs xpack.security enabled (off in this lab): JSON audit events for authentication/authorization/run_as")

    def log_level_runtime(self, client: EsClient) -> str:
        client._req("PUT", "/_cluster/settings", {"transient": {"logger.org.elasticsearch.snapshots": "DEBUG"}})
        seen = client._req("GET", "/_cluster/settings").get("transient", {}).get("logger", {}).get("org", {}).get("elasticsearch", {}).get("snapshots")
        client._req("PUT", "/_cluster/settings", {"transient": {"logger.org.elasticsearch.snapshots": None}})
        if seen != "DEBUG":
            raise EngineError(f"logger setting not applied: {seen}")
        return "PUT _cluster/settings transient logger.org.elasticsearch.snapshots=DEBUG -> verified -> reset (no restart)"

    def _mappings(self, coll: str) -> dict:
        common = {"index": {"number_of_shards": self.features.get("shards", 1), "number_of_replicas": self.features.get("replicas", 0),
                            "refresh_interval": "30s"}}
        m: dict[str, Any] = {"settings": common, "mappings": {"properties": {}}}
        props = m["mappings"]["properties"]
        if coll == "customers":
            props.update({"id": {"type": "integer"}, "email": {"type": "keyword"}, "country_code": {"type": "keyword"}, "tier": {"type": "keyword"},
                          "name": {"type": "text"}, "created_at": {"type": "date"}, "location": {"type": "geo_point"},
                          "attrs": {"properties": {"lang": {"type": "keyword"}, "age": {"type": "integer"}, "newsletter": {"type": "boolean"}}},
                          "last_login": {"type": "keyword"}})
        elif coll == "categories":
            props.update({"id": {"type": "integer"}, "parent_id": {"type": "integer"}, "name": {"type": "keyword"}, "depth": {"type": "integer"}})
        elif coll == "products":
            props.update({"id": {"type": "integer"}, "category_id": {"type": "integer"}, "sku": {"type": "keyword"}, "name": {"type": "text"},
                          "description": {"type": "text"}, "price": {"type": "double"}, "active": {"type": "boolean"},
                          "attrs": {"properties": {"color": {"type": "keyword"}, "material": {"type": "keyword"}, "weight": {"type": "double"}, "tags": {"type": "keyword"}}},
                          "embedding": {"type": "dense_vector", "dims": VEC_DIM, "index": True, "similarity": "cosine"}})
        elif coll == "orders":
            m["settings"]["index"]["max_result_window"] = 300000     # allow deep from/size so opt03 can compare offset vs search_after
            props.update({"id": {"type": "integer"}, "customer_id": {"type": "integer"}, "status": {"type": "keyword"}, "ordered_at": {"type": "date"},
                          "total_amount": {"type": "double"}, "shipping_country": {"type": "keyword"}, "items": {"type": "object", "enabled": False}})
        elif coll == "order_items":
            props.update({"id": {"type": "integer"}, "order_id": {"type": "integer"}, "product_id": {"type": "integer"}, "qty": {"type": "integer"}, "unit_price": {"type": "double"}})
        elif coll == "events":
            props.update({"id": {"type": "long"}, "customer_id": {"type": "integer"}, "event_type": {"type": "keyword"}, "occurred_at": {"type": "date"},
                          "value_num": {"type": "double"}, "payload": {"type": "object", "enabled": False}, "expires_at": {"type": "date"}, "blob": {"type": "keyword", "index": False, "doc_values": False, "ignore_above": 2147483647}})
        elif coll == "inventory":
            props.update({"product_id": {"type": "integer"}, "warehouse_id": {"type": "integer"}, "qty": {"type": "integer"}, "updated_at": {"type": "date"}, "version": {"type": "integer"}})
        return m

    def create_schema(self, client: EsClient) -> list[str]:
        done = []
        for coll in self.collections():
            client._req("PUT", f"/{client.idx(coll)}", self._mappings(coll))
            done.append(f"index {client.idx(coll)} ({len(self._mappings(coll)['mappings']['properties'])} mapped fields)")
        return done

    def after_load(self, client: EsClient) -> list[str]:
        client.refresh()
        return ["_refresh all indices (search visibility)"]

    def create_index(self, client: EsClient, name: str, coll: str, fields: list[str]) -> str:
        raise Unsupported("all fields are indexed at mapping time; opt01 (add a missing index) does not apply")

    def index_variants(self):
        # every field is already indexed; the interesting comparison is filter (cached, no scoring) vs query (scored) —
        # but to keep the matrix comparable we show the single indexed path
        return [("indexed_field (all fields indexed by default)", ["event_type", "occurred_at"])]

    def drop_index(self, client: EsClient, name: str, coll: str) -> None:
        pass

    def durability_levels(self) -> list[str]:
        return ["refresh_false", "refresh_true", "wait_for"]

    def read_levels(self) -> list[tuple[str, str | None]]:
        return [("default", None)]

    def max_connections(self, client: EsClient) -> int | None:
        try:
            r = client._req("GET", "/_nodes/thread_pool", params={"filter_path": "nodes.*.thread_pool.write.queue_size"})
            return None
        except Exception:  # noqa: BLE001
            return None

    def replication_status(self, client: EsClient) -> dict[str, Any]:
        try:
            h = client._req("GET", "/_cluster/health")
            out = {"status": h.get("status"), "nodes": h.get("number_of_nodes"), "active_shards": h.get("active_shards"),
                   "relocating": h.get("relocating_shards"), "initializing": h.get("initializing_shards"), "unassigned": h.get("unassigned_shards")}
            try:
                out["master"] = client._req("GET", "/_cat/master", params={"format": "json"})[0].get("node")
                shards = client._req("GET", f"/_cat/shards/{self.prefix}*", params={"format": "json", "h": "node,prirep,state"})
                per: dict[str, dict[str, int]] = {}
                for sh in shards:
                    d = per.setdefault(sh.get("node") or "unassigned", {"p": 0, "r": 0})
                    d["p" if sh.get("prirep") == "p" else "r"] += 1
                out["shards_per_node"] = per
            except Exception:  # noqa: BLE001
                pass
            return out
        except Exception as e:  # noqa: BLE001
            return {"error": str(e)[:100]}

    def create_probe_collection(self, client: EsClient) -> None:
        """The replication phase waits for the probe index on every node: create it up front (with the lab's replica count)."""
        idx = client.idx("repl_probe")
        try:
            client._req("PUT", f"/{idx}", {"settings": {"index": {"number_of_shards": self.features.get("shards", 1),
                                                                  "number_of_replicas": self.features.get("replicas", 0), "refresh_interval": "1s"}}})
        except EngineError as e:
            if "resource_already_exists" not in str(e):
                raise
        client._req("GET", "/_cluster/health", params={"wait_for_status": "yellow", "timeout": "60s", "index": idx})

    def leader_target(self) -> Target | None:
        """The member currently elected master (node.name == the lab's container short name, e.g. es2 -> nslab-es2)."""
        c = self.connect_primary(timeout=10)
        try:
            master = c._req("GET", "/_cat/master", params={"format": "json"})[0].get("node")
        finally:
            c.close()
        for t in self.cfg.targets:
            if t.container and master and t.container.endswith(master):
                return t
        return None

    def promote(self, target: Target) -> str:
        return "master re-elected by the remaining quorum; replica shards promoted to primary automatically"
