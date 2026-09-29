"""Cassandra backend for the 'messaging' scenario (query-first, one table per access path).

Keyspace `chatter` (RF=3). Connections are token-aware across all nodes (not pinned) so load spreads like a real
client. Messages use the Discord bucketing model: ((conversation_id, bucket), message_id DESC). The inbox is a
per-user partition of conversation rows carrying a materialised last-message pointer (fan-out-on-write) or just
membership (fan-in-on-read). Unread is a counter table; typing rows use TTL; friend-accept is a LOGGED BATCH;
username registration is an LWT.
"""
from __future__ import annotations

import time
from typing import Any

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import UNSET_VALUE, BatchStatement, BatchType, SimpleStatement, tuple_factory

try:
    from cassandra.io.libevreactor import LibevConnection
except ImportError:  # pragma: no cover
    LibevConnection = None

from ..config import StackConfig
from .corpus import BUCKET_MS, EPOCH_MS, T_END, bucket_of, sf_ts, snowflake
from .messaging import MessagingBackend

KS = "chatter"
# Live writes use a "logical now" = the end of the seeded time window, so live messages land in the same recent
# bucket as the tail of the backlog and history reads (anchored at T_END) see both — independent of the wall clock.
LOGICAL_NOW = T_END

DDL = [
    "CREATE TABLE IF NOT EXISTS users (user_id bigint PRIMARY KEY, username text, region text, created_ms bigint)",
    "CREATE TABLE IF NOT EXISTS users_by_username (username text PRIMARY KEY, user_id bigint)",
    "CREATE TABLE IF NOT EXISTS friends_by_user (user_id bigint, friend_id bigint, since_ms bigint, PRIMARY KEY ((user_id), friend_id))",
    "CREATE TABLE IF NOT EXISTS friend_req_in (user_id bigint, from_id bigint, requested_ms bigint, PRIMARY KEY ((user_id), from_id))",
    "CREATE TABLE IF NOT EXISTS friend_req_out (user_id bigint, to_id bigint, requested_ms bigint, PRIMARY KEY ((user_id), to_id))",
    "CREATE TABLE IF NOT EXISTS conversations (conversation_id bigint PRIMARY KEY, ctype text, created_ms bigint, member_count int)",
    "CREATE TABLE IF NOT EXISTS conv_members (conversation_id bigint, user_id bigint, PRIMARY KEY ((conversation_id), user_id))",
    "CREATE TABLE IF NOT EXISTS conv_members_by_user (user_id bigint, conversation_id bigint, PRIMARY KEY ((user_id), conversation_id))",
    "CREATE TABLE IF NOT EXISTS messages_by_conv (conversation_id bigint, bucket int, message_id bigint, sender_id bigint, body text, "
    "PRIMARY KEY ((conversation_id, bucket), message_id)) WITH CLUSTERING ORDER BY (message_id DESC)",
    "CREATE TABLE IF NOT EXISTS inbox_by_user (user_id bigint, conversation_id bigint, last_ms bigint, last_message_id bigint, last_sender bigint, "
    "PRIMARY KEY ((user_id), conversation_id))",
    "CREATE TABLE IF NOT EXISTS unread (user_id bigint, conversation_id bigint, cnt counter, PRIMARY KEY ((user_id), conversation_id))",
    "CREATE TABLE IF NOT EXISTS receipts (conversation_id bigint, user_id bigint, last_read_id bigint, PRIMARY KEY ((conversation_id), user_id))",
    "CREATE TABLE IF NOT EXISTS typing (conversation_id bigint, user_id bigint, PRIMARY KEY ((conversation_id), user_id))",
]
TABLES = ["users", "users_by_username", "friends_by_user", "friend_req_in", "friend_req_out", "conversations",
          "conv_members", "conv_members_by_user", "messages_by_conv", "inbox_by_user", "unread", "receipts", "typing"]


def buckets_desc(n: int = 6, now_ms: int | None = None) -> list[int]:
    """The most-recent `n` bucket numbers (newest first) — history reads walk these until they have enough rows."""
    b = bucket_of(now_ms if now_ms is not None else (EPOCH_MS + 2 * 365 * 86_400 * 1000))
    return [b - i for i in range(n) if b - i >= 0]


class CassandraMessagingBackend(MessagingBackend):
    def __init__(self, cfg: StackConfig, fanout: str = "write"):
        self.cfg = cfg
        self.fanout = fanout
        self.cluster = None
        self.session = None
        self._ps: dict[str, Any] = {}
        rep = cfg.features.get("consistency", "LOCAL_QUORUM")
        self.cl = getattr(ConsistencyLevel, rep, ConsistencyLevel.LOCAL_QUORUM)
        self.rf = int(cfg.features.get("replication_factor", 3))
        self.fanout_cap = int(cfg.features.get("fanout_cap", 500))
        self._seq = 0

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    # --- lifecycle -----------------------------------------------------------------------------
    def connect(self, keyspace: str = KS) -> None:
        hosts = [t.host for t in self.cfg.targets if t.role in ("primary", "node")]
        profile = ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc="dc1")),
                                   request_timeout=120, consistency_level=self.cl, row_factory=tuple_factory)
        kw = {}
        if LibevConnection is not None:
            kw["connection_class"] = LibevConnection
        self.cluster = Cluster(contact_points=hosts, port=self.cfg.primary.port or 9042,
                               execution_profiles={EXEC_PROFILE_DEFAULT: profile}, connect_timeout=15, **kw)
        self.session = self.cluster.connect()
        try:
            self.session.set_keyspace(keyspace)
        except Exception:  # noqa: BLE001 - keyspace may not exist yet (before create_schema)
            pass

    def close(self) -> None:
        try:
            self.cluster.shutdown()
        except Exception:  # noqa: BLE001
            pass

    def _p(self, cql: str, cl: ConsistencyLevel | None = None):
        st = self._ps.get(cql)
        if st is None:
            st = self.session.prepare(cql)
            st.consistency_level = cl if cl is not None else self.cl
            self._ps[cql] = st
        return st

    def server_info(self) -> str:
        r = self.session.execute("SELECT release_version FROM system.local", timeout=15).one()
        return f"Cassandra {r[0]} keyspace={KS} RF={self.rf} CL={self.cl} fanout={self.fanout}"

    def create_schema(self) -> list[str]:
        self.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KS} WITH replication = "
                             f"{{'class': 'NetworkTopologyStrategy', 'replication_factor': {self.rf}}}", timeout=60)
        self.session.set_keyspace(KS)
        done = []
        for d in DDL:
            self.session.execute(d, timeout=60)
            done.append(d.split("(")[0].strip())
        return done

    def drop_schema(self) -> None:
        self.session.execute(f"DROP KEYSPACE IF EXISTS {KS}", timeout=120)

    def counts(self) -> dict[str, int]:
        out = {}
        for t in ("users", "friends_by_user", "conversations", "messages_by_conv"):
            try:
                out[t] = int(self.session.execute(SimpleStatement(f"SELECT COUNT(*) FROM {t}", consistency_level=ConsistencyLevel.ONE), timeout=300).one()[0])
            except Exception as e:  # noqa: BLE001
                out[t] = f"err: {e}"[:60]
        return out

    # --- seed (bulk, CL=ONE for speed) ------------------------------------------------------------
    def _many(self, cql: str, rows: list[tuple], concurrency: int = 128) -> int:
        st = self._p(cql, ConsistencyLevel.ONE)
        rows = [tuple(UNSET_VALUE if v is None else v for v in r) for r in rows]
        for ok, res in execute_concurrent_with_args(self.session, st, rows, concurrency=concurrency, raise_on_first_error=True):
            if not ok:
                raise res
        return len(rows)

    def seed_users(self, rows):
        n = self._many("INSERT INTO users (user_id, username, region, created_ms) VALUES (?, ?, ?, ?)", rows)
        self._many("INSERT INTO users_by_username (username, user_id) VALUES (?, ?)", [(r[1], r[0]) for r in rows])
        return n

    def seed_friends(self, rows):
        return self._many("INSERT INTO friends_by_user (user_id, friend_id, since_ms) VALUES (?, ?, ?)", rows)

    def seed_requests(self, rows):
        self._many("INSERT INTO friend_req_in (user_id, from_id, requested_ms) VALUES (?, ?, ?)", rows)
        return self._many("INSERT INTO friend_req_out (user_id, to_id, requested_ms) VALUES (?, ?, ?)", [(r[1], r[0], r[2]) for r in rows])

    def seed_conversation(self, cid, ctype, members, created_ms):
        self.session.execute(self._p("INSERT INTO conversations (conversation_id, ctype, created_ms, member_count) VALUES (?, ?, ?, ?)", ConsistencyLevel.ONE),
                             (cid, ctype, created_ms, len(members)))
        self._many("INSERT INTO conv_members (conversation_id, user_id) VALUES (?, ?)", [(cid, u) for u in members])
        self._many("INSERT INTO conv_members_by_user (user_id, conversation_id) VALUES (?, ?)", [(u, cid) for u in members])

    def seed_messages(self, cid, members, rows):
        # rows: (message_id, sender, ts_ms, body). Also materialise each member's inbox pointer to the last message.
        payload = [(cid, bucket_of(ts), mid, sender, body) for (mid, sender, ts, body) in rows]
        n = self._many("INSERT INTO messages_by_conv (conversation_id, bucket, message_id, sender_id, body) VALUES (?, ?, ?, ?, ?)", payload)
        if rows:
            mid, sender, ts, _ = rows[-1]
            self._many("INSERT INTO inbox_by_user (user_id, conversation_id, last_ms, last_message_id, last_sender) VALUES (?, ?, ?, ?, ?)",
                       [(u, cid, ts, mid, sender) for u in members])
        return n

    # --- write operations -------------------------------------------------------------------------
    def register_user(self, uid, username) -> bool:
        r = self.session.execute(self._p("INSERT INTO users_by_username (username, user_id) VALUES (?, ?) IF NOT EXISTS", ConsistencyLevel.QUORUM),
                                 (username, uid)).one()
        applied = bool(r[0])
        if applied:
            self.session.execute(self._p("INSERT INTO users (user_id, username, region, created_ms) VALUES (?, ?, 'live', ?)"),
                                 (uid, username, int(time.time() * 1000)))
        return applied

    def send_message(self, cid, members, sender, body) -> int:
        import os
        seq = ((os.getpid() & 0x3FF) << 12) | (self._next_seq() & 0xFFF)   # unique-enough 22-bit seq across workers
        now = LOGICAL_NOW
        mid = snowflake(now, seq)
        self.session.execute(self._p("INSERT INTO messages_by_conv (conversation_id, bucket, message_id, sender_id, body) VALUES (?, ?, ?, ?, ?)"),
                             (cid, bucket_of(now), mid, sender, body))
        if self.fanout == "write":
            fan = members[:self.fanout_cap]
            inbox = self._p("INSERT INTO inbox_by_user (user_id, conversation_id, last_ms, last_message_id, last_sender) VALUES (?, ?, ?, ?, ?)")
            unread = self._p("UPDATE unread SET cnt = cnt + 1 WHERE user_id = ? AND conversation_id = ?")
            execute_concurrent_with_args(self.session, inbox, [(u, cid, now, mid, sender) for u in fan], concurrency=64, raise_on_first_error=True)
            execute_concurrent_with_args(self.session, unread, [(u, cid) for u in fan if u != sender], concurrency=64, raise_on_first_error=False)
        return mid

    def mark_read(self, uid, cid, up_to_message_id) -> None:
        # counters cannot be set; "reset" = subtract the current value (a documented Cassandra limitation)
        row = self.session.execute(self._p("SELECT cnt FROM unread WHERE user_id = ? AND conversation_id = ?"), (uid, cid)).one()
        cur = row[0] if row and row[0] else 0
        if cur:
            self.session.execute(self._p("UPDATE unread SET cnt = cnt - ? WHERE user_id = ? AND conversation_id = ?"), (cur, uid, cid))
        self.session.execute(self._p("INSERT INTO receipts (conversation_id, user_id, last_read_id) VALUES (?, ?, ?)"), (cid, uid, up_to_message_id))

    def typing(self, cid, uid) -> None:
        self.session.execute(self._p("INSERT INTO typing (conversation_id, user_id) VALUES (?, ?) USING TTL 10", ConsistencyLevel.ONE), (cid, uid))

    def send_friend_request(self, frm, to) -> None:
        now = int(time.time() * 1000)
        b = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.cl)
        b.add(self._p("INSERT INTO friend_req_in (user_id, from_id, requested_ms) VALUES (?, ?, ?)"), (to, frm, now))
        b.add(self._p("INSERT INTO friend_req_out (user_id, to_id, requested_ms) VALUES (?, ?, ?)"), (frm, to, now))
        self.session.execute(b)

    def accept_friend_request(self, uid, other) -> bool:
        exists = self.session.execute(self._p("SELECT from_id FROM friend_req_in WHERE user_id = ? AND from_id = ?"), (uid, other)).one()
        now = int(time.time() * 1000)
        b = BatchStatement(batch_type=BatchType.LOGGED, consistency_level=self.cl)
        b.add(self._p("INSERT INTO friends_by_user (user_id, friend_id, since_ms) VALUES (?, ?, ?)"), (uid, other, now))
        b.add(self._p("INSERT INTO friends_by_user (user_id, friend_id, since_ms) VALUES (?, ?, ?)"), (other, uid, now))
        b.add(self._p("DELETE FROM friend_req_in WHERE user_id = ? AND from_id = ?"), (uid, other))
        b.add(self._p("DELETE FROM friend_req_out WHERE user_id = ? AND to_id = ?"), (other, uid))
        self.session.execute(b)
        return exists is not None

    # --- read / serve operations ------------------------------------------------------------------
    def load_history(self, cid, limit, before_id=None):
        out: list[tuple] = []
        for b in buckets_desc():
            if before_id is None:
                rows = self.session.execute(self._p("SELECT message_id, sender_id, body FROM messages_by_conv WHERE conversation_id = ? AND bucket = ? LIMIT ?"),
                                            (cid, b, limit))
            else:
                rows = self.session.execute(self._p("SELECT message_id, sender_id, body FROM messages_by_conv WHERE conversation_id = ? AND bucket = ? AND message_id < ? LIMIT ?"),
                                            (cid, b, before_id, limit))
            out.extend((r[0], r[1]) for r in rows)
            if len(out) >= limit:
                return out[:limit]
        return out

    def load_inbox(self, uid, limit):
        if self.fanout == "write":
            rows = list(self.session.execute(self._p("SELECT conversation_id, last_ms, last_message_id, last_sender FROM inbox_by_user WHERE user_id = ?"), (uid,)))
            rows.sort(key=lambda r: (-(r[1] or 0), r[0]))
            return [(r[0], r[1]) for r in rows[:limit]]
        # fan-in-on-read: membership then each conversation's head message (concurrent), sort client-side
        convs = [r[0] for r in self.session.execute(self._p("SELECT conversation_id FROM conv_members_by_user WHERE user_id = ?"), (uid,))]
        head = self._p("SELECT conversation_id, message_id FROM messages_by_conv WHERE conversation_id = ? AND bucket = ? LIMIT 1")
        b0 = buckets_desc()[0]
        res = execute_concurrent_with_args(self.session, head, [(c, b0) for c in convs], concurrency=64, raise_on_first_error=False)
        rows = []
        for ok, r in res:
            if ok:
                one = r.one()
                if one:
                    rows.append((one[0], sf_ts(one[1])))
        rows.sort(key=lambda x: -x[1])
        return rows[:limit]

    def unread_badge(self, uid, limit):
        rows = self.session.execute(self._p("SELECT conversation_id, cnt FROM unread WHERE user_id = ?"), (uid,))
        out = [(r[0], int(r[1])) for r in rows if r[1]]
        out.sort(key=lambda x: -x[1])
        return out[:limit]

    def list_friends(self, uid, limit):
        rows = self.session.execute(self._p("SELECT friend_id FROM friends_by_user WHERE user_id = ? LIMIT ?"), (uid, limit))
        return [(r[0],) for r in rows]

    def mutual_friends(self, a, b):
        fa = {r[0] for r in self.session.execute(self._p("SELECT friend_id FROM friends_by_user WHERE user_id = ? LIMIT 1000"), (a,))}
        fb = {r[0] for r in self.session.execute(self._p("SELECT friend_id FROM friends_by_user WHERE user_id = ? LIMIT 1000"), (b,))}
        return sorted(fa & fb)[:100]

    def people_you_may_know(self, uid, scan_cap=200, top=10):
        friends = [r[0] for r in self.session.execute(self._p("SELECT friend_id FROM friends_by_user WHERE user_id = ? LIMIT ?"), (uid, scan_cap))]
        fset = set(friends)
        fof = self._p("SELECT friend_id FROM friends_by_user WHERE user_id = ? LIMIT 500")
        res = execute_concurrent_with_args(self.session, fof, [(f,) for f in friends], concurrency=64, raise_on_first_error=False)
        counts: dict[int, int] = {}
        for ok, r in res:
            if not ok:
                continue
            for row in r:
                cand = row[0]
                if cand != uid and cand not in fset:
                    counts[cand] = counts.get(cand, 0) + 1
        return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:top]
