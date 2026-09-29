"""Scenario layer: realistic, domain-shaped workloads that go beyond the generic operation catalog.

A Scenario defines an engine-agnostic set of operations + a seeded corpus; a Backend implements those operations
against one engine; a Driver runs a weighted, hotspot-skewed, multi-process load and records per-operation percentiles,
throughput, per-container resource use and analysis curves. First scenario: `messaging` ("Chatter").
"""
