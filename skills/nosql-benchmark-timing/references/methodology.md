# NoSQL benchmark methodology — pitfalls found while building this lab

- **Refresh vs write on search engines.** Right after `_bulk`, `_count` returned stale numbers and the load check
  "failed" — Elasticsearch counts are post-refresh. Refresh (or wait) before asserting document counts or reading back.
- **Bound NULL = tombstone (Cassandra).** Loading with `None`-bound columns wrote 100k+ tombstones; a later
  `ALLOW FILTERING` scan then failed with `ReadFailure` ("scanned over N tombstones"). Bind `UNSET_VALUE` for absent
  columns. Tombstones also silently slow reads long before they fail.
- **Vector-index ingestion is concurrency-sensitive.** Cassandra 5.0 SAI/jvector throws "outstanding pooled objects
  beyond the limit" above ~32 in-flight writes to a vectored table — load vectors at lower concurrency than plain rows.
- **Embedded stores can be opened once per process.** LMDB/RocksDB refuse a second open of the same file; the load phase
  forks, so parallel loading needs each process to own the file or (as here) load single-process and share one handle
  across threads for the read phases.
- **`IN` is not a batch read on a coordinator engine.** A 100-key `IN` on Cassandra serialises partitions in the
  coordinator (~70 ms); 100 concurrent single-partition reads finish in ~5 ms. Batch by concurrency, not by list length.
- **Deep offset pagination is a hard error on ES** past `index.max_result_window` (10 000) — raise the window only to
  *measure* offset vs `search_after`; in production use `search_after`.
- **The GIL caps in-process throughput.** Python threads plateaued; the lab forks processes for server engines and
  reports embedded engines at their single-thread peak (the honest number for a Python caller).
- **Failover downtime depends on election tuning.** MongoDB replica-set default election + driver server-selection retry
  gave ~30 s here; Redis Sentinel (`down-after 2s`) ~2.5 s; Cassandra leaderless ~0.9 s (no election). Report the config.
