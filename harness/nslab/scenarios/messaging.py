"""The 'messaging' scenario ("Chatter"): engine-agnostic operation set + backend contract.

A Backend implements these methods against one engine; the driver (driver.py) calls them with corpus-derived,
hotspot-skewed targets. The Cassandra implementation is `cassandra_messaging.CassandraMessagingBackend`; the same
contract lets ScyllaDB reuse it and MongoDB/Redis implement it later for a cross-engine comparison.

fanout modes:
  "write" — fan-out-on-write: a send updates every member's materialised inbox row + unread counter (cheap reads,
            write amplification that grows with group size).
  "read"  — fan-in-on-read: a send writes only the message; load_inbox recomputes each conversation's head at read
            time (cheap writes, expensive inbox).
"""
from __future__ import annotations

import multiprocessing as mp
import random
import time
from abc import ABC, abstractmethod
from typing import Any

from ..util import summarize
from .base import Ctx, Op, Scenario, register
from .corpus import Corpus, Sizes, T_START, T_END


class MessagingBackend(ABC):
    """One engine's implementation of the Chatter store. `fanout` is set by the driver/CLI."""
    fanout: str = "write"

    # --- lifecycle / schema -----------------------------------------------------------
    @abstractmethod
    def connect(self) -> None: ...
    @abstractmethod
    def close(self) -> None: ...
    @abstractmethod
    def create_schema(self) -> list[str]: ...
    @abstractmethod
    def drop_schema(self) -> None: ...
    @abstractmethod
    def server_info(self) -> str: ...

    # --- seed (bulk, called by shard) -------------------------------------------------
    @abstractmethod
    def seed_users(self, rows: list[tuple[int, str, str, int]]) -> int: ...            # (uid, username, region, created_ms)
    @abstractmethod
    def seed_friends(self, rows: list[tuple[int, int, int]]) -> int: ...               # (uid, friend_id, since_ms)
    @abstractmethod
    def seed_requests(self, rows: list[tuple[int, int, int]]) -> int: ...              # (uid, from_id, requested_ms)
    @abstractmethod
    def seed_conversation(self, cid: int, ctype: str, members: list[int], created_ms: int) -> None: ...
    @abstractmethod
    def seed_messages(self, cid: int, members: list[int], rows: list[tuple[int, int, int, str]]) -> int: ...  # (message_id, sender, ts_ms, body)
    @abstractmethod
    def counts(self) -> dict[str, int]: ...

    # --- operations (write path) ------------------------------------------------------
    @abstractmethod
    def register_user(self, uid: int, username: str) -> bool: ...                       # LWT; returns applied
    @abstractmethod
    def send_message(self, cid: int, members: list[int], sender: int, body: str) -> int: ...   # returns message_id
    @abstractmethod
    def mark_read(self, uid: int, cid: int, up_to_message_id: int) -> None: ...
    @abstractmethod
    def typing(self, cid: int, uid: int) -> None: ...
    @abstractmethod
    def send_friend_request(self, frm: int, to: int) -> None: ...
    @abstractmethod
    def accept_friend_request(self, uid: int, other: int) -> bool: ...                  # returns whether a request existed

    # --- operations (read / serve path) -----------------------------------------------
    @abstractmethod
    def load_history(self, cid: int, limit: int, before_id: int | None = None) -> list[tuple]: ...
    @abstractmethod
    def load_inbox(self, uid: int, limit: int) -> list[tuple]: ...
    @abstractmethod
    def unread_badge(self, uid: int, limit: int) -> list[tuple]: ...
    @abstractmethod
    def list_friends(self, uid: int, limit: int) -> list[tuple]: ...
    @abstractmethod
    def mutual_friends(self, a: int, b: int) -> list[int]: ...
    @abstractmethod
    def people_you_may_know(self, uid: int, scan_cap: int = 200, top: int = 10) -> list[tuple]: ...


# --- helpers to pick realistic, hotspot-skewed targets -------------------------------------------
def _conv_and_sender(ctx: Ctx) -> tuple[int, list[int], int]:
    cid = ctx.corpus.random_conversation(ctx.rng)
    _, members = ctx.corpus.members_of(cid)
    return cid, members, ctx.corpus.sender_of(cid, members, ctx.rng)


def op_send_message(ctx: Ctx):
    cid, members, sender = _conv_and_sender(ctx)
    return ctx.backend.send_message(cid, members, sender, ctx.corpus.body(ctx.rng))


def op_load_history(ctx: Ctx):
    cid = ctx.corpus.random_conversation(ctx.rng)
    return ctx.backend.load_history(cid, limit=50)


def op_load_inbox(ctx: Ctx):
    uid = ctx.corpus.random_user(ctx.rng)
    return ctx.backend.load_inbox(uid, limit=30)


def op_mark_read(ctx: Ctx):
    cid, members, _ = _conv_and_sender(ctx)
    uid = members[ctx.rng.randrange(len(members))]
    return ctx.backend.mark_read(uid, cid, up_to_message_id=0)


def op_unread_badge(ctx: Ctx):
    return ctx.backend.unread_badge(ctx.corpus.random_user(ctx.rng), limit=50)


def op_typing(ctx: Ctx):
    cid, members, uid = _conv_and_sender(ctx)
    return ctx.backend.typing(cid, uid)


def op_send_request(ctx: Ctx):
    frm = ctx.corpus.uniform_user(ctx.rng)
    to = ctx.corpus.random_user(ctx.rng)
    if frm == to:
        to = 1 + (to % ctx.corpus.s.users)
    return ctx.backend.send_friend_request(frm, to)


def op_accept_request(ctx: Ctx):
    uid = ctx.corpus.random_user(ctx.rng)
    pend = ctx.corpus.seed_pending_requests(uid)
    if not pend:
        return None
    other = pend[ctx.rng.randrange(len(pend))][0]
    return ctx.backend.accept_friend_request(uid, other)


def op_list_friends(ctx: Ctx):
    return ctx.backend.list_friends(ctx.corpus.random_user(ctx.rng), limit=200)


def op_mutual_friends(ctx: Ctx):
    a = ctx.corpus.random_user(ctx.rng)
    friends = ctx.backend.list_friends(a, limit=50)
    b = friends[ctx.rng.randrange(len(friends))][0] if friends else ctx.corpus.random_user(ctx.rng)
    return ctx.backend.mutual_friends(a, b)


def op_pymk(ctx: Ctx):
    return ctx.backend.people_you_may_know(ctx.corpus.random_user(ctx.rng))


def op_register(ctx: Ctx):
    # register into a high id range so it never collides with the seeded space (measures the LWT fast path)
    uid = ctx.corpus.s.users + 1 + ctx.rng.randrange(50_000_000)
    return ctx.backend.register_user(uid, f"live_{uid}")


DEFAULT_MIX: list[Op] = [
    Op("load_history",   25, "read",  op_load_history,  ("serve", "wide_partition")),
    Op("load_inbox",     20, "read",  op_load_inbox,    ("serve", "fanout")),
    Op("send_message",   15, "write", op_send_message,  ("ingest", "fanout")),
    Op("mark_read",      12, "write", op_mark_read,     ("counter",)),
    Op("unread_badge",    8, "read",  op_unread_badge,  ("counter", "serve")),
    Op("typing",          5, "write", op_typing,        ("ttl",)),
    Op("list_friends",    4, "read",  op_list_friends,  ("social",)),
    Op("send_request",    3, "write", op_send_request,  ("social",)),
    Op("accept_request",  2, "write", op_accept_request,("social", "batch")),
    Op("mutual_friends",  2, "read",  op_mutual_friends,("social",)),
    Op("people_you_may_know", 2, "read", op_pymk,       ("social", "fanout_read")),
    Op("register",        1, "write", op_register,      ("lwt",)),
]

MIX_BY_NAME = {o.name: o for o in DEFAULT_MIX}


def make_backend(engine_key: str, cfg, fanout: str = "write") -> MessagingBackend:
    if cfg.driver == "cql":
        from .cassandra_messaging import CassandraMessagingBackend
        return CassandraMessagingBackend(cfg, fanout=fanout)
    raise NotImplementedError(f"messaging scenario has no backend for driver '{cfg.driver}' (stack {engine_key}) yet")


# =============================== seed (multi-process) ==================================================
def _seed_worker(stack_key: str, size: str, w: int, workers: int, q: mp.Queue) -> None:
    from ..config import load_stack
    cfg = load_stack(stack_key)
    corpus = Corpus(Sizes.named(size))
    be = make_backend(stack_key, cfg)
    be.connect()
    s = corpus.s
    counts = {"users": 0, "friends": 0, "requests": 0, "conversations": 0, "messages": 0}
    t0 = time.time()
    try:
        ubuf, rbuf = [], []
        for uid in range(1 + w, s.users + 1, workers):
            r = random.Random(uid)
            region = r.choice(["US", "DE", "GB", "TR", "FR", "IN", "BR", "JP"])
            ubuf.append((uid, corpus.username(uid), region, T_START + int(r.random() * (T_END - T_START))))
            for frm, ts in corpus.seed_pending_requests(uid):
                rbuf.append((uid, frm, ts))
            if len(ubuf) >= 4000:
                counts["users"] += be.seed_users(ubuf); ubuf = []
            if len(rbuf) >= 4000:
                counts["requests"] += be.seed_requests(rbuf); rbuf = []
        if ubuf:
            counts["users"] += be.seed_users(ubuf)
        if rbuf:
            counts["requests"] += be.seed_requests(rbuf)
        fbuf = []
        for i, (a, b) in enumerate(corpus.edges()):
            if i % workers != w:
                continue
            fbuf.append((a, b, T_START)); fbuf.append((b, a, T_START))
            if len(fbuf) >= 6000:
                counts["friends"] += be.seed_friends(fbuf); fbuf = []
        if fbuf:
            counts["friends"] += be.seed_friends(fbuf)
        for cid in range(1 + w, s.conversations + 1, workers):
            ctype, members = corpus.members_of(cid)
            be.seed_conversation(cid, ctype, members, T_START)
            counts["conversations"] += 1
            msgs = list(corpus.messages(cid))
            if msgs:
                counts["messages"] += be.seed_messages(cid, members, msgs)
        q.put({"w": w, "counts": counts, "seconds": round(time.time() - t0, 1), "error": None})
    except Exception as e:  # noqa: BLE001
        q.put({"w": w, "counts": counts, "seconds": round(time.time() - t0, 1), "error": f"{type(e).__name__}: {e}"[:300]})
    finally:
        be.close()


def seed(stack_key: str, size: str, workers: int = 8, *, log=print) -> dict[str, Any]:
    from ..config import load_stack
    cfg = load_stack(stack_key)
    be = make_backend(stack_key, cfg); be.connect()
    log("  dropping + creating keyspace ...")
    be.drop_schema()
    schema = be.create_schema()
    log(f"  schema: {len(schema)} tables")
    be.close()
    sizes = Sizes.named(size)
    log(f"  seeding size={size}: {sizes.users:,} users, {sizes.conversations:,} conversations, ~{sizes.avg_degree} avg friend degree, {workers} workers")
    ctx = mp.get_context("fork")
    q: mp.Queue = ctx.Queue()
    procs = [ctx.Process(target=_seed_worker, args=(stack_key, size, w, workers, q), daemon=True) for w in range(workers)]
    t0 = time.time()
    for p in procs:
        p.start()
    results = [q.get() for _ in procs]
    for p in procs:
        p.join(timeout=10)
    total = {"users": 0, "friends": 0, "requests": 0, "conversations": 0, "messages": 0}
    errs = []
    for r in results:
        for k, v in r["counts"].items():
            total[k] += v
        if r["error"]:
            errs.append(r["error"])
    secs = time.time() - t0
    be = make_backend(stack_key, cfg); be.connect(); server = be.server_info(); be.close()
    out = {"size": size, "sizes": vars(sizes), "seconds": round(secs, 1), "written": total,
           "messages_per_s": round(total["messages"] / secs) if secs else None, "errors": errs, "server": server}
    log(f"  seeded in {secs:.1f}s: {total} ({out['messages_per_s']} msgs/s)")
    if errs:
        log(f"  seed errors: {errs[:2]}")
    return out


def curves(stack_key: str, size: str, opts: dict, *, samples: int = 400, log=print) -> dict[str, Any]:
    """send latency vs conversation group size, and PYMK latency vs friend degree."""
    from ..config import load_stack
    cfg = load_stack(stack_key)
    corpus = Corpus(Sizes.named(size))
    be = make_backend(stack_key, cfg, fanout=opts.get("fanout", "write")); be.connect()
    rng = random.Random(7)
    log(f"  fan-out send latency vs group size ({samples} samples) ...")
    send_pts, pymk_pts = [], []
    for _ in range(samples):
        cid = corpus.random_conversation(rng)
        _, members = corpus.members_of(cid)
        sender = members[rng.randrange(len(members))]
        t0 = time.perf_counter_ns()
        try:
            be.send_message(cid, members, sender, corpus.body(rng))
            send_pts.append((len(members), (time.perf_counter_ns() - t0) / 1e6))
        except Exception:  # noqa: BLE001
            pass
    log(f"  PYMK latency vs friend degree ({samples} samples) ...")
    for _ in range(samples):
        uid = corpus.random_user(rng)
        deg = len(be.list_friends(uid, limit=1000))
        t0 = time.perf_counter_ns()
        try:
            be.people_you_may_know(uid)
            pymk_pts.append((deg, (time.perf_counter_ns() - t0) / 1e6))
        except Exception:  # noqa: BLE001
            pass
    be.close()

    def bucketize(pts, edges, labels):
        b = {lb: [] for lb in labels}
        for sz_, ms in pts:
            for e, lb in zip(edges, labels):
                if sz_ <= e:
                    b[lb].append(ms); break
            else:
                b[labels[-1]].append(ms)
        return {lb: {"n": len(v), **({k: round(val, 3) for k, val in summarize(v).items() if k in ("p50", "p95", "p99", "max")} if v else {})} for lb, v in b.items()}

    out = {"send_vs_group_size": bucketize(send_pts, [2, 5, 20, 50, 150, 10**9], ["dm(2)", "3-5", "6-20", "21-50", "51-150", "150+"]),
           "pymk_vs_degree": bucketize(pymk_pts, [10, 50, 150, 500, 10**9], ["<=10", "11-50", "51-150", "151-500", "500+"]), "opts": opts}
    for k in ("send_vs_group_size", "pymk_vs_degree"):
        log(f"  {k}: " + "  ".join(f"{lb}:{b.get('p95','-')}ms(n{b.get('n',0)})" for lb, b in out[k].items()))
    return out


register(Scenario(
    name="messaging",
    sizes_named=Sizes.named,
    make_corpus=lambda s: Corpus(s),
    make_backend=lambda stack, cfg, opts: make_backend(stack, cfg, fanout=(opts or {}).get("fanout", "write")),
    mix=lambda: DEFAULT_MIX,
    seed=seed,
    curves=curves,
    default_opts={"fanout": "write"},
    blurb="Chatter — a social messaging store (register/find-friends + 1:1 & group chat).",
))
