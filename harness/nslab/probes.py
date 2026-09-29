"""Capability probe catalog (the NoSQL equivalent of rd-databases' DDL/DML feature probes).

Each probe is executed by `Client.probe_<name>()` on the engine adapter:
  - returns a dict/str          -> supported (the return value is stored as `detail`)
  - raises ops.Unsupported      -> unsupported
  - raises anything else        -> error (accepted but failed at run time; error text stored)
  - method missing              -> unsupported
Adapters may also declare `STATIC_PROBES = {name: (status, detail)}` for facts that cannot be exercised
from a client (e.g. "sharding built in").
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Probe:
    name: str
    title: str
    group: str            # indexing | consistency | model | operations | limits | logging


PROBES: list[Probe] = [
    Probe("secondary_index", "Create a secondary index on a non-key field and use it", "indexing"),
    Probe("compound_index", "Compound / composite index over two fields", "indexing"),
    Probe("unique_constraint", "Unique constraint on a non-key field (duplicate rejected)", "indexing"),
    Probe("ttl_expiry", "Native per-document/key expiry", "indexing"),
    Probe("text_index", "Full-text index with tokenised search", "indexing"),
    Probe("geo_index", "Geospatial index and radius query", "indexing"),
    Probe("vector_index", "Vector (ANN) index and kNN query", "indexing"),
    Probe("partial_update", "Update a single field without rewriting the document", "model"),
    Probe("atomic_increment", "Server-side atomic numeric increment", "consistency"),
    Probe("optimistic_concurrency", "Compare-and-set / version or ETag-guarded write", "consistency"),
    Probe("multi_doc_transaction", "Multi-document / multi-key ACID transaction with rollback", "consistency"),
    Probe("tunable_consistency", "Per-request consistency / write concern / read preference", "consistency"),
    Probe("nested_query", "Filter on a field inside a nested object", "model"),
    Probe("array_operators", "Query and update array-valued fields (contains, push)", "model"),
    Probe("aggregation", "Server-side group-by aggregation", "model"),
    Probe("server_side_join", "Join / lookup between two collections inside the engine", "model"),
    Probe("graph_traversal", "Variable-depth traversal over references", "model"),
    Probe("schema_validation", "Server-enforced document schema / constraints", "model"),
    Probe("change_streams", "Change feed / watch / keyspace notifications", "operations"),
    Probe("server_side_scripting", "Stored procedures / Lua / JS / UDF executed by the server", "operations"),
    Probe("pubsub", "Publish/subscribe messaging built into the engine", "operations"),
    Probe("explain_plan", "Query plan / profile output for a query", "operations"),
    Probe("bulk_import", "Native bulk import path (bulk API, batch, COPY-like tool)", "operations"),
    Probe("large_document", "Store and read back a 15 MB document / value", "limits"),
    Probe("many_fields", "Document with 5000 top-level fields", "limits"),
    Probe("read_from_replica", "Client can direct reads to a replica / secondary", "operations"),
    Probe("snapshot_backup", "Online snapshot / backup command available", "operations"),
    Probe("incremental_backup", "Incremental / differential backup (file- or segment-level)", "operations"),
    Probe("pitr", "Point-in-time recovery from a continuous change log", "operations"),
    Probe("slow_query_log", "Server-side slow-query log with a configurable threshold", "logging"),
    Probe("audit_log", "Audit / full-query log of statements or auth events", "logging"),
    Probe("structured_logs", "JSON-structured server log out of the box", "logging"),
    Probe("runtime_log_level", "Change log verbosity at run time (no restart)", "logging"),
]

PROBE_BY_NAME = {p.name: p for p in PROBES}
