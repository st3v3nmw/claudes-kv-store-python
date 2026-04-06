"""
Distributed Key-Value Store — leader-election + persistence stage.

Run: python server.py
Env: DATA_DIR, PEERS (comma-separated peer addresses)

Architecture
------------
Built on aiohttp (single event loop).  All Raft state lives as module-level
globals; mutations are protected by a single asyncio.Lock (_raft_lock), which
is cooperative and has near-zero overhead when uncontended.

Persistence (files under DATA_DIR):
  raft_state.json  — current_term, voted_for          (fsync on vote/term change)
  raft_log.jsonl   — log entries, one JSON line each  (group-commit fsync per batch)
  snapshot.json    — materialised KV dict              (fsync on SIGTERM)

Recovery: load snapshot → load raft_state → replay all log entries.

Write path:
  HTTP handlers enqueue (command, Future) on _write_queue and await the Future.
  A single _write_worker task drains the queue, appends entries, applies them,
  then fsyncs in a thread-pool executor (non-blocking to event loop) before
  resolving all Futures in the batch.  This naturally batches every write that
  arrived while the previous fsync was in flight.

Raft timing:
  Election timeout : 500–1000 ms (randomised)
  Heartbeat        : 100 ms
  RPC timeout      : 150 ms (heartbeat), 400 ms (vote)
"""

import asyncio
import json
import logging
import os
import random
import socket
import time

import aiohttp
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [raft] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("raft")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR: str       = os.environ.get("DATA_DIR", "")
PEERS:    list[str] = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]
ADDR:     str       = os.environ.get("ADDR", "")

ELECTION_TIMEOUT_MIN  = 0.5    # seconds
ELECTION_TIMEOUT_MAX  = 1.0
HEARTBEAT_INTERVAL    = 0.1    # seconds — 1/5 of election timeout min
HEARTBEAT_RPC_TIMEOUT = 0.2    # seconds — generous but well below election timeout
VOTE_RPC_TIMEOUT      = 0.4    # seconds — must complete within election timeout min
LEADER_STALE_THRESHOLD = 0.3   # seconds — 3x heartbeat, << election timeout min
HEARTBEAT_MISS_LIMIT  = 1      # consecutive missed rounds before leader steps down

RAFT_STATE_FILE = "raft_state.json"
RAFT_LOG_FILE   = "raft_log.jsonl"
SNAPSHOT_FILE   = "snapshot.json"

# ---------------------------------------------------------------------------
# Self-address detection
# ---------------------------------------------------------------------------

def _detect_self_addr() -> str:
    if ADDR:
        return ADDR
    if PEERS:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((PEERS[0].split(":")[0], 80))
            ip = s.getsockname()[0]
            s.close()
            return f"{ip}:8080"
        except Exception:
            pass
    try:
        return f"{socket.gethostbyname(socket.gethostname())}:8080"
    except Exception:
        return "127.0.0.1:8080"

SELF_ADDR: str = _detect_self_addr()

# ---------------------------------------------------------------------------
# In-memory state
#
# All mutations must hold _raft_lock (asyncio.Lock).  Since asyncio is
# cooperative, holding the lock across an await simply suspends other lock
# waiters — it never blocks the event loop thread.
# ---------------------------------------------------------------------------

_raft_lock: asyncio.Lock   # initialised in on_startup (needs running loop)

store:         dict[str, str] = {}
_raft_log:     list[dict]     = []
_commit_index: int             = 0
_last_applied: int             = 0

_current_term: int        = 0
_voted_for:    str | None = None

_role:                str        = "follower"
_leader:              str | None = None
_leader_last_contact: float      = 0.0

_next_index:  dict[str, int] = {}
_match_index: dict[str, int] = {}

_election_task: asyncio.Task | None = None

# ---------------------------------------------------------------------------
# Log helpers  (caller holds _raft_lock)
# ---------------------------------------------------------------------------

def _log_last_index() -> int:
    return len(_raft_log)

def _log_last_term() -> int:
    return _raft_log[-1]["term"] if _raft_log else 0

def _log_term_at(index: int) -> int:
    if index <= 0 or index > len(_raft_log):
        return 0
    return _raft_log[index - 1]["term"]

def _log_append_mem(term: int, command: dict) -> tuple[int, dict]:
    entry = {"index": len(_raft_log) + 1, "term": term, **command}
    _raft_log.append(entry)
    return entry["index"], entry

def _log_truncate_after(index: int) -> None:
    del _raft_log[index:]

def _apply_committed_entries() -> None:
    global _last_applied
    while _last_applied < _commit_index:
        entry = _raft_log[_last_applied]
        _last_applied += 1
        op = entry.get("op")
        if op == "put":
            store[entry["key"]] = entry["value"]
        elif op == "delete":
            store.pop(entry["key"], None)
        elif op == "clear":
            store.clear()

# ---------------------------------------------------------------------------
# Persistence helpers
# ---------------------------------------------------------------------------

def _raft_state_path() -> str:
    return os.path.join(DATA_DIR, RAFT_STATE_FILE)

def _log_path() -> str:
    return os.path.join(DATA_DIR, RAFT_LOG_FILE)

def _snapshot_path() -> str:
    return os.path.join(DATA_DIR, SNAPSHOT_FILE)


async def _save_raft_state() -> None:
    """Persist current_term and voted_for with fsync via thread-pool executor."""
    if not DATA_DIR:
        return
    data = {"current_term": _current_term, "voted_for": _voted_for}
    def _write() -> None:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = _raft_state_path() + ".tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, _raft_state_path())
    await asyncio.get_running_loop().run_in_executor(None, _write)


def _load_raft_state() -> None:
    global _current_term, _voted_for
    if not DATA_DIR:
        return
    path = _raft_state_path()
    if not os.path.exists(path):
        return
    try:
        with open(path) as f:
            s = json.load(f)
        _current_term = s.get("current_term", 0)
        _voted_for    = s.get("voted_for", None)
    except Exception:
        pass


# WAL file state — only touched by _write_worker (via queue) and _rewrite_log_file.
_log_fh = None  # open append file handle

def _write_log_line(entry: dict) -> None:
    """Buffer one log entry.  No fsync — caller will await fsync via executor."""
    global _log_fh
    if not DATA_DIR:
        return
    if _log_fh is None:
        os.makedirs(DATA_DIR, exist_ok=True)
        _log_fh = open(_log_path(), "a", encoding="utf-8")
    _log_fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _do_fsync() -> None:
    """Flush + fsync the WAL file.  Called via run_in_executor."""
    if _log_fh is None:
        return
    _log_fh.flush()
    os.fsync(_log_fh.fileno())


def _rewrite_log_file() -> None:
    """Atomically rewrite the whole log file from _raft_log.
    Blocks event loop briefly; called only after follower log truncation (rare)."""
    global _log_fh
    if not DATA_DIR:
        return
    if _log_fh is not None:
        _log_fh.close()
        _log_fh = None
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = _log_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for entry in _raft_log:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, _log_path())


def _load_log_file() -> None:
    if not DATA_DIR:
        return
    path = _log_path()
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for raw in f:
            raw = raw.strip()
            if not raw:
                continue
            try:
                _raft_log.append(json.loads(raw))
            except json.JSONDecodeError:
                break  # truncated tail from a crash


def _load_snapshot() -> None:
    if not DATA_DIR:
        return
    path = _snapshot_path()
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            store.update(json.load(f))
    except Exception:
        pass


async def _checkpoint() -> None:
    """Flush WAL and write snapshot.  Called on graceful shutdown."""
    if not DATA_DIR:
        return
    loop = asyncio.get_running_loop()
    if _log_fh is not None:
        await loop.run_in_executor(None, _do_fsync)
    snapshot = dict(store)

    def _write() -> None:
        os.makedirs(DATA_DIR, exist_ok=True)
        tmp = _snapshot_path() + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, _snapshot_path())

    await loop.run_in_executor(None, _write)

# ---------------------------------------------------------------------------
# Election timer
# ---------------------------------------------------------------------------

def _reset_election_timer() -> None:
    global _election_task
    if _election_task is not None:
        _election_task.cancel()
    timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    _election_task = asyncio.create_task(
        _election_timeout_coro(timeout), name="election-timer"
    )

def _cancel_election_timer() -> None:
    global _election_task
    if _election_task is not None:
        _election_task.cancel()
        _election_task = None

async def _election_timeout_coro(timeout: float) -> None:
    try:
        await asyncio.sleep(timeout)
    except asyncio.CancelledError:
        return
    async with _raft_lock:
        if _role != "leader":
            _start_election()
    await _save_raft_state()  # outside lock

# ---------------------------------------------------------------------------
# Role transitions  (caller holds _raft_lock)
# ---------------------------------------------------------------------------

def _step_down(new_term: int, new_leader: str | None = None) -> bool:
    """Update in-memory state only.  Returns True if term changed (caller must
    await _save_raft_state() after releasing _raft_lock)."""
    global _current_term, _voted_for, _role, _leader, _next_index, _match_index
    need_persist = new_term > _current_term
    if need_persist:
        _current_term = new_term
        _voted_for    = None
        _next_index   = {}
        _match_index  = {}
    _role   = "follower"
    _leader = new_leader
    _reset_election_timer()
    log.info("step_down term=%d leader=%s", _current_term, new_leader)
    return need_persist


def _become_leader() -> None:
    global _role, _leader, _next_index, _match_index
    _role        = "leader"
    _leader      = SELF_ADDR
    _next_index  = {p: _log_last_index() + 1 for p in PEERS}
    _match_index = {p: 0 for p in PEERS}
    _cancel_election_timer()
    log.info("became leader term=%d", _current_term)
    asyncio.create_task(_broadcast_initial_heartbeat(), name="initial-heartbeat")


def _start_election() -> None:
    """Update in-memory state and fire the election task.  Caller holds
    _raft_lock and must await _save_raft_state() after releasing it."""
    global _current_term, _voted_for, _role, _leader
    _current_term += 1
    _role         = "candidate"
    _voted_for    = SELF_ADDR
    _leader       = None
    _reset_election_timer()
    log.info("election started term=%d", _current_term)
    asyncio.create_task(
        _run_election(_current_term, list(PEERS), _log_last_index(), _log_last_term()),
        name=f"election-t{_current_term}",
    )

# ---------------------------------------------------------------------------
# Outbound RPCs
# ---------------------------------------------------------------------------

_http_session: aiohttp.ClientSession  # initialised in on_startup


async def _rpc_request_vote(peer: str, term: int,
                             last_log_index: int, last_log_term: int) -> tuple[bool, int]:
    try:
        async with _http_session.post(
            f"http://{peer}/raft/request-vote",
            json={
                "term":           term,
                "candidate-id":   SELF_ADDR,
                "last-log-index": last_log_index,
                "last-log-term":  last_log_term,
            },
            timeout=aiohttp.ClientTimeout(total=VOTE_RPC_TIMEOUT),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("vote-granted", False), data.get("term", 0)
    except Exception:
        pass
    return False, 0


async def _rpc_append_entries(peer: str, term: int,
                               prev_log_index: int, prev_log_term: int,
                               entries: list, leader_commit: int) -> tuple[bool, int]:
    try:
        async with _http_session.post(
            f"http://{peer}/raft/append-entries",
            json={
                "term":           term,
                "leader-id":      SELF_ADDR,
                "prev-log-index": prev_log_index,
                "prev-log-term":  prev_log_term,
                "entries":        entries,
                "leader-commit":  leader_commit,
            },
            timeout=aiohttp.ClientTimeout(total=HEARTBEAT_RPC_TIMEOUT),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("success", False), data.get("term", 0)
    except Exception:
        pass
    return False, 0

# ---------------------------------------------------------------------------
# Election runner
# ---------------------------------------------------------------------------

async def _run_election(term: int, peers: list[str],
                        last_log_index: int, last_log_term: int) -> None:
    cluster_size = len(peers) + 1
    quorum       = cluster_size // 2 + 1

    if not peers:
        async with _raft_lock:
            if _role == "candidate" and _current_term == term:
                _become_leader()
        return

    votes: set[str] = {SELF_ADDR}

    async def vote_from(peer: str) -> tuple[str, bool, int]:
        granted, peer_term = await _rpc_request_vote(peer, term, last_log_index, last_log_term)
        return peer, granted, peer_term

    tasks = [asyncio.create_task(vote_from(p)) for p in peers]
    try:
        for coro in asyncio.as_completed(tasks):
            try:
                peer, granted, peer_term = await coro
            except Exception:
                continue

            action = None
            async with _raft_lock:
                if _role != "candidate" or _current_term != term:
                    return
                if peer_term > _current_term:
                    _step_down(peer_term)
                    action = "step_down"
                elif granted:
                    votes.add(peer)
                    log.info("vote from %s term=%d votes=%d/%d",
                             peer, term, len(votes), quorum)
                    if len(votes) >= quorum:
                        _become_leader()
                        action = "became_leader"

            if action == "step_down":
                await _save_raft_state()
                return
            if action == "became_leader":
                return
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

async def _broadcast_initial_heartbeat() -> None:
    term = _current_term
    for peer in list(PEERS):
        asyncio.create_task(_fire_heartbeat(peer, term, 0, 0, [], 0))


async def _fire_heartbeat(peer: str, term: int, prev_idx: int, prev_trm: int,
                          entries: list, commit: int) -> None:
    _, peer_term = await _rpc_append_entries(peer, term, prev_idx, prev_trm, entries, commit)
    if peer_term > term:
        save = False
        async with _raft_lock:
            if peer_term > _current_term:
                save = _step_down(peer_term)
        if save:
            await _save_raft_state()


async def _heartbeat_round(term: int) -> tuple[int, int]:
    peers = list(PEERS)
    if not peers:
        return 1, 0

    cluster_size = len(peers) + 1
    quorum       = cluster_size // 2 + 1
    ack_count    = 1
    max_term     = 0

    async with _raft_lock:
        commit    = _commit_index
        peer_args = {
            p: (
                _next_index.get(p, _log_last_index() + 1) - 1,
                _log_term_at(_next_index.get(p, _log_last_index() + 1) - 1),
                [],
                commit,
            )
            for p in peers
        }

    tasks = [
        asyncio.create_task(_rpc_append_entries(p, term, *args))
        for p, args in peer_args.items()
    ]
    # Always wait for ALL peers — breaking early cancels outstanding RPCs and
    # deprives slow followers of heartbeats, causing spurious elections.
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, Exception):
            continue
        ok, peer_term = result
        if peer_term > max_term:
            max_term = peer_term
        if ok:
            ack_count += 1

    return ack_count, max_term


async def _heartbeat_loop() -> None:
    """Fire periodic heartbeats while leader."""
    consecutive_misses = 0
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        async with _raft_lock:
            if _role != "leader":
                consecutive_misses = 0
                continue
            term = _current_term

        try:
            ack_count, max_term = await _heartbeat_round(term)
        except Exception:
            log.exception("heartbeat_round raised")
            ack_count, max_term = 0, 0

        save = False
        async with _raft_lock:
            if _role != "leader" or _current_term != term:
                consecutive_misses = 0
                continue
            if max_term > _current_term:
                save = _step_down(max_term)
                consecutive_misses = 0
            else:
                cluster_size = len(PEERS) + 1
                quorum       = cluster_size // 2 + 1
                if ack_count < quorum:
                    consecutive_misses += 1
                    log.info("quorum miss %d/%d (%d/%d acks) term=%d",
                             consecutive_misses, HEARTBEAT_MISS_LIMIT,
                             ack_count, quorum, _current_term)
                    if consecutive_misses >= HEARTBEAT_MISS_LIMIT:
                        log.info("quorum lost — stepping down term=%d", _current_term)
                        _step_down(_current_term)
                        consecutive_misses = 0
                else:
                    consecutive_misses = 0
        if save:
            await _save_raft_state()

# ---------------------------------------------------------------------------
# Write actor
#
# HTTP handlers put (command, Future) on the queue and await the Future.
# _write_worker drains the queue in batches, appends + applies under the lock,
# then fsyncs in a thread-pool executor before resolving all callers.
# ---------------------------------------------------------------------------

_write_queue: asyncio.Queue  # initialised in on_startup
_STOP = object()             # sentinel: tells _write_worker to exit cleanly


async def _write_worker() -> None:
    loop = asyncio.get_running_loop()
    global _commit_index
    while True:
        first = await _write_queue.get()
        if first is _STOP:
            return
        batch = [first]
        while not _write_queue.empty():
            try:
                item = _write_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            if item is _STOP:
                _write_queue.put_nowait(_STOP)  # re-queue so outer loop sees it
                break
            batch.append(item)

        entries = []
        try:
            async with _raft_lock:
                for command, _ in batch:
                    idx, entry = _log_append_mem(_current_term, command)
                    _commit_index = idx
                    entries.append(entry)
                _apply_committed_entries()
                if DATA_DIR:
                    for entry in entries:
                        _write_log_line(entry)
            # Lock released before fsync — Raft RPCs and elections proceed freely.
            if DATA_DIR:
                await loop.run_in_executor(None, _do_fsync)
        finally:
            # Always resolve futures so HTTP handlers get a response, even on
            # cancellation.  _checkpoint() in on_shutdown ensures durability.
            for _, future in batch:
                if not future.done():
                    future.set_result(None)


async def _leader_write(command: dict) -> None:
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    await _write_queue.put((command, future))
    await future

# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

def _leader_redirect(request: web.Request) -> web.Response | None:
    """Return a redirect/503 if not leader, else None."""
    if _role == "leader":
        return None
    if _leader is not None and (time.monotonic() - _leader_last_contact) <= LEADER_STALE_THRESHOLD:
        return web.Response(
            status=307,
            headers={"Location": f"http://{_leader}{request.rel_url}"},
        )
    return web.Response(text="no leader", status=503)


async def handle_kv(request: web.Request) -> web.Response:
    key = request.match_info.get("key", "")
    if not key:
        return web.Response(text="key cannot be empty", status=400)

    redir = _leader_redirect(request)
    if redir is not None:
        return redir

    if request.method == "PUT":
        value = await request.text()
        if not value:
            return web.Response(text="value cannot be empty", status=400)
        await _leader_write({"op": "put", "key": key, "value": value})
        return web.Response(status=200)

    if request.method == "GET":
        value = store.get(key)
        if value is None:
            return web.Response(text="key not found", status=404)
        return web.Response(text=value, status=200)

    if request.method == "DELETE":
        await _leader_write({"op": "delete", "key": key})
        return web.Response(status=200)

    return web.Response(text="method not allowed", status=405)


async def handle_clear(request: web.Request) -> web.Response:
    if request.method != "DELETE":
        return web.Response(text="method not allowed", status=405)
    redir = _leader_redirect(request)
    if redir is not None:
        return redir
    await _leader_write({"op": "clear"})
    return web.Response(status=200)


async def handle_health(_: web.Request) -> web.Response:
    return web.Response(text="ok")


async def handle_cluster_info(_: web.Request) -> web.Response:
    leader = _leader
    if _role != "leader" and (
        leader is None or (time.monotonic() - _leader_last_contact) > LEADER_STALE_THRESHOLD
    ):
        leader = None
    return web.Response(
        text=json.dumps({
            "id":     SELF_ADDR,
            "role":   _role,
            "term":   _current_term,
            "leader": leader,
            "peers":  sorted(PEERS),
        }),
        content_type="application/json",
    )


async def handle_request_vote(request: web.Request) -> web.Response:
    global _voted_for
    body      = await request.json()
    cand_term = body["term"]
    cand_id   = body["candidate-id"]
    cand_lli  = body.get("last-log-index", 0)
    cand_llt  = body.get("last-log-term",  0)

    grant      = False
    need_save  = False
    resp_term  = 0

    async with _raft_lock:
        # Disruptive-server prevention (Raft §6)
        if (cand_term > _current_term
                and _role == "follower"
                and _leader is not None
                and (time.monotonic() - _leader_last_contact) < LEADER_STALE_THRESHOLD):
            return web.Response(
                text=json.dumps({"term": _current_term, "vote-granted": False}),
                content_type="application/json",
            )

        if cand_term > _current_term:
            need_save = _step_down(cand_term)

        my_lli = _log_last_index()
        my_llt = _log_last_term()
        log_ok = (cand_llt > my_llt) or (cand_llt == my_llt and cand_lli >= my_lli)

        grant = (
            cand_term >= _current_term
            and (_voted_for is None or _voted_for == cand_id)
            and log_ok
        )
        if grant:
            _voted_for = cand_id
            _reset_election_timer()
            log.info("voted for %s term=%d", cand_id, _current_term)
            need_save = True

        resp_term = _current_term

    # Fsync outside the lock — lock not held during disk I/O.
    # Must complete before we respond (Raft safety: don't ack a vote we haven't persisted).
    if need_save:
        await _save_raft_state()

    return web.Response(
        text=json.dumps({"term": resp_term, "vote-granted": grant}),
        content_type="application/json",
    )


async def handle_append_entries(request: web.Request) -> web.Response:
    global _current_term, _role, _leader, _leader_last_contact, _commit_index

    body          = await request.json()
    leader_term   = body["term"]
    leader_id     = body["leader-id"]
    prev_log_idx  = body.get("prev-log-index", 0)
    prev_log_trm  = body.get("prev-log-term",  0)
    entries       = body.get("entries", [])
    leader_commit = body.get("leader-commit", 0)

    need_save = False
    success   = False
    resp_term = 0

    async with _raft_lock:
        if leader_term < _current_term:
            return web.Response(
                text=json.dumps({"term": _current_term, "success": False}),
                content_type="application/json",
            )

        if leader_term > _current_term:
            need_save = _step_down(leader_term, leader_id)
        else:
            if _role == "candidate":
                log.info("candidate yielding to leader %s term=%d", leader_id, leader_term)
                _role = "follower"
            _leader = leader_id
            _reset_election_timer()

        _leader_last_contact = time.monotonic()
        resp_term = _current_term

        if prev_log_idx > 0 and _log_term_at(prev_log_idx) != prev_log_trm:
            pass  # success stays False; still need to persist term if it changed
        else:
            for i, entry in enumerate(entries):
                slot = prev_log_idx + 1 + i
                if slot <= _log_last_index():
                    if _log_term_at(slot) != entry["term"]:
                        _log_truncate_after(slot - 1)
                        _raft_log.append(entry)
                else:
                    _raft_log.append(entry)

            if entries:
                _rewrite_log_file()  # rare; blocks event loop briefly

            if leader_commit > _commit_index:
                _commit_index = min(leader_commit, _log_last_index())
                _apply_committed_entries()

            success = True

    if need_save:
        await _save_raft_state()

    return web.Response(
        text=json.dumps({"term": resp_term, "success": success}),
        content_type="application/json",
    )

# ---------------------------------------------------------------------------
# App lifecycle
# ---------------------------------------------------------------------------

async def on_startup(app: web.Application) -> None:
    global _http_session, _raft_lock, _write_queue
    global _current_term, _voted_for, _commit_index

    _raft_lock   = asyncio.Lock()
    _write_queue = asyncio.Queue()
    _http_session = aiohttp.ClientSession()

    _load_snapshot()
    _load_raft_state()
    _load_log_file()

    async with _raft_lock:
        _commit_index = _log_last_index()
        _apply_committed_entries()

    app["write_worker"] = asyncio.create_task(_write_worker(),   name="write-worker")
    app["heartbeat"]    = asyncio.create_task(_heartbeat_loop(), name="heartbeat")

    log.info("starting node=%s peers=%s", SELF_ADDR, PEERS)

    need_save = False
    async with _raft_lock:
        if not PEERS:
            _current_term += 1
            _voted_for     = SELF_ADDR
            _become_leader()
            need_save = True
        else:
            _reset_election_timer()
    if need_save:
        await _save_raft_state()


async def on_shutdown(app: web.Application) -> None:
    _cancel_election_timer()

    # Drain the write queue: send sentinel and wait for the worker to process
    # all preceding items and exit.  In-flight HTTP writes get their response
    # before the server closes connections.
    await _write_queue.put(_STOP)
    write_worker = app.get("write_worker")
    if write_worker:
        try:
            await asyncio.wait_for(write_worker, timeout=10.0)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            write_worker.cancel()
            await asyncio.gather(write_worker, return_exceptions=True)

    heartbeat = app.get("heartbeat")
    if heartbeat:
        heartbeat.cancel()
        await asyncio.gather(heartbeat, return_exceptions=True)

    await _checkpoint()
    await _http_session.close()

# ---------------------------------------------------------------------------
# App factory + entry point
# ---------------------------------------------------------------------------

def make_app() -> web.Application:
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app.router.add_route("*", "/kv/{key}", handle_kv)
    app.router.add_route("*", "/kv/",     handle_kv)
    app.router.add_route("*", "/clear",            handle_clear)
    app.router.add_get("/health",                  handle_health)
    app.router.add_get("/cluster/info",            handle_cluster_info)
    app.router.add_post("/raft/request-vote",      handle_request_vote)
    app.router.add_post("/raft/append-entries",    handle_append_entries)
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host="0.0.0.0", port=8080, access_log=None)
