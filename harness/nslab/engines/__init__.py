"""Driver-level engine adapters. `make_engine(stack)` picks the adapter from lab.yaml `driver`."""
from __future__ import annotations

from ..config import StackConfig
from .base import Client, Engine, EngineError

_REGISTRY = {
    "resp": ("resp", "RespEngine"),
    "pymongo": ("mongo", "MongoEngine"),
    "cql": ("cql", "CqlEngine"),
    "bolt": ("bolt", "BoltEngine"),
    "es": ("es", "EsEngine"),
    "couchdb": ("couchdb", "CouchEngine"),
    "arango": ("arango", "ArangoEngine"),
    "etcd": ("etcd", "EtcdEngine"),
    "memcached": ("memcached", "MemcachedEngine"),
    "meili": ("meili", "MeiliEngine"),
    "typesense": ("typesense", "TypesenseEngine"),
    "influx": ("influx", "InfluxEngine"),
    "qdrant": ("qdrant", "QdrantEngine"),
    "weaviate": ("weaviate", "WeaviateEngine"),
    "milvus": ("milvus", "MilvusEngine"),
    "chroma": ("chroma", "ChromaEngine"),
    "surreal": ("surreal", "SurrealEngine"),
    "couchbase": ("couchbase", "CouchbaseEngine"),
    "aerospike": ("aerospike", "AerospikeEngine"),
    "tarantool": ("tarantool", "TarantoolEngine"),
    "ravendb": ("ravendb", "RavenEngine"),
    "falkordb": ("falkordb", "FalkorEngine"),
    "lmdb": ("embedded", "LmdbEngine"),
    "rocksdb": ("embedded", "RocksEngine"),
}


def make_engine(cfg: StackConfig) -> Engine:
    d = cfg.driver
    if d not in _REGISTRY:
        raise EngineError(f"unknown driver '{d}' for stack {cfg.key} (known: {sorted(_REGISTRY)})")
    mod, cls = _REGISTRY[d]
    import importlib
    m = importlib.import_module(f".{mod}", __name__)
    return getattr(m, cls)(cfg)


__all__ = ["make_engine", "Engine", "Client", "EngineError"]
