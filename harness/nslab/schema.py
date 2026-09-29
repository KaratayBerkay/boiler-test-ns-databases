"""Portable benchmark data model.

The same e-commerce entity set as the relational lab (rd-databases), expressed as JSON documents so that
document, key-value, wide-column, graph, search, time-series and vector engines can all load it and the
numbers stay comparable across the two labs.

Every collection has a primary key (`key`) and a list of *access paths* the workload uses; adapters map
those onto whatever the engine offers (secondary index, denormalised table, hash slot, node label, ...).
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Collection:
    name: str
    key: tuple[str, ...]                                   # primary key field(s)
    fields: tuple[str, ...]                                # top-level fields (documentation + CSV order)
    ts_fields: tuple[str, ...] = ()                        # ISO-8601 in NDJSON, datetime in memory
    nested: tuple[str, ...] = ()                           # object-valued fields
    access_paths: tuple[tuple[str, ...], ...] = ()         # secondary lookups the workload needs (baseline indexes)
    embedded_from: str | None = None                       # 'order_items' rows are embedded into 'orders.items'
    text_field: str | None = None                          # full-text search target
    geo_field: str | None = None                           # {lat, lon} field
    vector_field: str | None = None                        # float[] field
    partition_hint: tuple[str, ...] = ()                   # wide-column partition key suggestion

    def key_of(self, doc: dict) -> str:
        return ":".join(str(doc[k]) for k in self.key)


COLLECTIONS: list[Collection] = [
    Collection("customers", ("id",),
               ("id", "name", "email", "country_code", "tier", "created_at", "attrs", "location"),
               ts_fields=("created_at",), nested=("attrs", "location"),
               access_paths=(("email",), ("country_code", "tier")), geo_field="location"),
    Collection("categories", ("id",), ("id", "parent_id", "name", "depth"), access_paths=(("parent_id",),)),
    Collection("products", ("id",),
               ("id", "category_id", "sku", "name", "description", "price", "attrs", "active", "embedding"),
               nested=("attrs",), access_paths=(("category_id",), ("sku",)), text_field="description", vector_field="embedding"),
    Collection("orders", ("id",),
               ("id", "customer_id", "status", "ordered_at", "total_amount", "shipping_country", "items"),
               ts_fields=("ordered_at",), access_paths=(("customer_id",), ("status", "ordered_at")),
               partition_hint=("customer_id",)),
    Collection("order_items", ("id",), ("id", "order_id", "product_id", "qty", "unit_price"),
               access_paths=(("order_id",), ("product_id",)), embedded_from="orders", partition_hint=("order_id",)),
    Collection("events", ("id",),
               ("id", "customer_id", "event_type", "occurred_at", "payload", "value_num"),
               ts_fields=("occurred_at",), nested=("payload",), access_paths=(("customer_id",),),
               partition_hint=("event_type",)),
    # NOTE: deliberately no baseline index on events(event_type, occurred_at): the optimisation phase adds it.
    Collection("inventory", ("product_id", "warehouse_id"),
               ("product_id", "warehouse_id", "qty", "updated_at", "version"),
               ts_fields=("updated_at",), partition_hint=("product_id",)),
]

COLLECTION_BY_NAME = {c.name: c for c in COLLECTIONS}
LOAD_ORDER = ["customers", "categories", "products", "orders", "order_items", "events", "inventory"]
CORE_COLLECTIONS = [c for c in LOAD_ORDER if c != "order_items"]      # engines that embed items skip the flat table
