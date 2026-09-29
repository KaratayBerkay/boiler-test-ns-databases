"""Deterministic social + messaging corpus for the 'messaging' scenario.

The corpus is the single source of truth for structure: the seeder and the load driver both derive users,
conversation membership, the message backlog and friendships from it (seeded RNGs keyed by id), so the driver can
pick realistic targets without querying the database. Hotspots are built in: user ids and conversation members are
sampled with a power-law skew, so low ids are "celebrities" that appear in many conversations and accrue huge
friend degree, and message counts per conversation are heavy-tailed (a few very hot conversations).
"""
from __future__ import annotations

import random
from dataclasses import dataclass

from ..datagen import WORDS

EPOCH_MS = 1_704_067_200_000                     # 2024-01-01T00:00:00Z
BUCKET_MS = 10 * 86_400 * 1000                    # 10-day message buckets (keeps a partition bounded)
T_START = EPOCH_MS
T_END = EPOCH_MS + 2 * 365 * 86_400 * 1000        # ~2 years


def snowflake(ts_ms: int, seq: int) -> int:
    """Time-sortable 64-bit id: (ms since epoch << 22) | seq."""
    return ((ts_ms - EPOCH_MS) << 22) | (seq & 0x3F_FFFF)


def sf_ts(mid: int) -> int:
    return (mid >> 22) + EPOCH_MS


def bucket_of(ts_ms: int) -> int:
    return (ts_ms - EPOCH_MS) // BUCKET_MS


def _rng(*parts: int) -> random.Random:
    h = 1469598103934665603
    for p in parts:
        h = ((h ^ (p & 0xFFFFFFFFFFFFFFFF)) * 1099511628211) & 0xFFFFFFFFFFFFFFFF
    return random.Random(h)


@dataclass(frozen=True)
class Sizes:
    users: int
    conversations: int
    group_frac: float = 0.30
    avg_degree: int = 30
    max_group: int = 400
    msg_cap: int = 4000            # heaviest conversation's message backlog (before the wide-partition variant)
    msg_skew: float = 5.0          # higher -> more conversations are small, few are huge
    user_skew: float = 2.4         # higher -> stronger celebrity concentration on low ids
    group_size_skew: float = 3.5

    @classmethod
    def named(cls, name: str) -> "Sizes":
        if name == "smoke":
            return cls(users=2_000, conversations=5_000, avg_degree=20, max_group=120, msg_cap=400)
        if name == "small":
            return cls(users=40_000, conversations=60_000, avg_degree=25, max_group=300, msg_cap=800)
        if name == "full":
            return cls(users=200_000, conversations=500_000, avg_degree=30, max_group=400, msg_cap=4000)
        raise ValueError(f"unknown size '{name}' (smoke|small|full)")


def zipf_id(r: random.Random, n: int, skew: float) -> int:
    """1..n, concentrated on low ids (celebrities) when skew>1."""
    return 1 + int((n - 1) * (r.random() ** skew))


class Corpus:
    def __init__(self, sizes: Sizes):
        self.s = sizes

    # --- users ------------------------------------------------------------------------
    def username(self, uid: int) -> str:
        return f"user{uid}"

    def random_user(self, r: random.Random) -> int:
        return zipf_id(r, self.s.users, self.s.user_skew)

    def uniform_user(self, r: random.Random) -> int:
        return 1 + int(r.random() * self.s.users)

    # --- conversations ----------------------------------------------------------------
    def random_conversation(self, r: random.Random) -> int:
        return zipf_id(r, self.s.conversations, self.s.user_skew)

    def members_of(self, cid: int) -> tuple[str, list[int]]:
        """Deterministic membership for a conversation (same result in seeder and driver)."""
        r = _rng(cid, 0xA11CE)
        if r.random() >= self.s.group_frac:
            a = zipf_id(r, self.s.users, self.s.user_skew)
            b = zipf_id(r, self.s.users, self.s.user_skew)
            while b == a:
                b = zipf_id(r, self.s.users, self.s.user_skew)
            return "dm", sorted((a, b))
        size = 3 + int((self.s.max_group - 3) * (r.random() ** self.s.group_size_skew))
        members = {self.uniform_user(r)}                       # a "creator"
        guard = 0
        while len(members) < size and guard < size * 5:
            members.add(zipf_id(r, self.s.users, self.s.user_skew))
            guard += 1
        return "group", sorted(members)

    def sender_of(self, cid: int, members: list[int], r: random.Random) -> int:
        return members[r.randrange(len(members))]

    # --- message backlog --------------------------------------------------------------
    def n_msgs(self, cid: int) -> int:
        r = _rng(cid, 0x3E55A)
        return 1 + int(self.s.msg_cap * (r.random() ** self.s.msg_skew))

    def body(self, r: random.Random) -> str:
        return " ".join(r.choice(WORDS) for _ in range(r.randint(3, 24)))

    def messages(self, cid: int):
        """Yield (message_id, sender_id, ts_ms, body) in ascending time for a conversation's backlog."""
        _, members = self.members_of(cid)
        n = self.n_msgs(cid)
        r = _rng(cid, 0x5E9)
        span = T_END - T_START
        step = max(1000, span // max(n, 1))
        ts = T_START + int(r.random() * span * 0.4)
        for j in range(n):
            ts = min(ts + int(r.random() * step) + 1000, T_END)
            yield snowflake(ts, j), members[r.randrange(len(members))], ts, self.body(r)

    # --- friendships (edge list) ------------------------------------------------------
    def n_edges(self) -> int:
        return self.s.users * self.s.avg_degree // 2

    def edges(self):
        """Yield undirected (a, b) edges; low ids accrue high degree (celebrities)."""
        r = _rng(0xF12E, self.s.users, self.s.avg_degree)
        for _ in range(self.n_edges()):
            a = self.uniform_user(r)
            b = zipf_id(r, self.s.users, self.s.user_skew)
            if a != b:
                yield (a, b)

    def seed_pending_requests(self, uid: int, k: int = 3):
        """A few deterministic pending inbound friend requests for a user (so `accept` has work)."""
        r = _rng(uid, 0x9E9DE)
        out = []
        for _ in range(k):
            frm = self.random_user(r)
            if frm != uid:
                out.append((frm, T_START + int(r.random() * (T_END - T_START))))
        return out
