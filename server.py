"""
Distributed Key-Value Store — leader-election stage.

Run: gunicorn --config gunicorn.conf.py server:app
Env: DATA_DIR, PEERS (comma-separated peer addresses, e.g. "10.0.0.2:8080,10.0.0.3:8080")

Persistence:
  - WAL with group-commit + fsync for crash durability (SIGKILL)
  - Snapshot on graceful shutdown (SIGTERM) via atexit
  - Raft currentTerm + votedFor persisted with fsync before responding to RPCs

Raft leader election:
  - Election timeout: 500–1000ms (randomized)
  - Heartbeat interval: 100ms
  - Quorum: majority of cluster size
  - Leader steps down immediately after any heartbeat round that fails to achieve quorum
"""

import atexit
import json
import logging
import os
import random
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests as http
from flask import Flask, request as freq, Response

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [raft] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("raft")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR: str = os.environ.get("DATA_DIR", "")
PEERS: list[str] = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]

SNAPSHOT_FILE = "snapshot.json"
WAL_FILE = "wal.jsonl"
RAFT_FILE = "raft_state.json"

ELECTION_TIMEOUT_MIN = 0.5   # seconds
ELECTION_TIMEOUT_MAX = 1.0
HEARTBEAT_INTERVAL = 0.1     # seconds
RPC_TIMEOUT = 0.15           # seconds — kept short so heartbeat rounds complete well within election timeout

# Followers stop redirecting to a leader they haven't heard from in this window.
LEADER_STALE_THRESHOLD = 2 * HEARTBEAT_INTERVAL  # 200 ms

# ---------------------------------------------------------------------------
# Self address detection
# ---------------------------------------------------------------------------

def _detect_self_addr() -> str:
    if PEERS:
        peer_ip = PEERS[0].split(":")[0]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((peer_ip, 80))
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
# KV store state
# ---------------------------------------------------------------------------

store: dict[str, str] = {}
kv_lock = threading.Lock()

# ---------------------------------------------------------------------------
# WAL with group-commit
# ---------------------------------------------------------------------------

_wal_cond = threading.Condition()
_wal_fh = None
_write_seq: int = 0
_synced_seq: int = 0
_sync_in_progress: bool = False


def _snapshot_path() -> str:
    return os.path.join(DATA_DIR, SNAPSHOT_FILE)


def _wal_path() -> str:
    return os.path.join(DATA_DIR, WAL_FILE)


def _raft_path() -> str:
    return os.path.join(DATA_DIR, RAFT_FILE)


def _append_wal(entry: dict) -> None:
    global _wal_fh, _write_seq, _synced_seq, _sync_in_progress

    line = json.dumps(entry, ensure_ascii=False) + "\n"
    fh = None
    target = 0

    with _wal_cond:
        if _wal_fh is None:
            os.makedirs(DATA_DIR, exist_ok=True)
            _wal_fh = open(_wal_path(), "a", encoding="utf-8")

        _wal_fh.write(line)
        _write_seq += 1
        my_seq = _write_seq

        while _synced_seq < my_seq:
            if not _sync_in_progress:
                _sync_in_progress = True
                target = _write_seq
                fh = _wal_fh
                break
            _wal_cond.wait()

    if fh is None:
        return

    try:
        fh.flush()
        os.fsync(fh.fileno())
    finally:
        with _wal_cond:
            _synced_seq = target
            _sync_in_progress = False
            _wal_cond.notify_all()


def _checkpoint() -> None:
    global _wal_fh

    if not DATA_DIR:
        return

    os.makedirs(DATA_DIR, exist_ok=True)

    with kv_lock:
        snapshot = dict(store)

    tmp = _snapshot_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    os.replace(tmp, _snapshot_path())

    with _wal_cond:
        if _wal_fh is not None:
            _wal_fh.close()
            _wal_fh = None

    open(_wal_path(), "w").close()


def _load_kv_data() -> None:
    if not DATA_DIR:
        return

    if os.path.exists(_snapshot_path()):
        with open(_snapshot_path(), "r", encoding="utf-8") as f:
            try:
                store.update(json.load(f))
            except json.JSONDecodeError:
                pass

    if os.path.exists(_wal_path()):
        with open(_wal_path(), "r", encoding="utf-8") as f:
            for raw in f:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    entry = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                op = entry.get("op")
                if op == "put":
                    store[entry["key"]] = entry["value"]
                elif op == "delete":
                    store.pop(entry["key"], None)
                elif op == "clear":
                    store.clear()


# ---------------------------------------------------------------------------
# Raft state
# ---------------------------------------------------------------------------

_raft_lock = threading.Lock()

# Persistent (must survive crashes)
_current_term: int = 0
_voted_for: str | None = None

# Volatile
_role: str = "follower"   # "follower" | "candidate" | "leader"
_leader: str | None = None
_leader_last_contact: float = 0.0   # monotonic time of last valid AppendEntries
_votes_received: set[str] = set()

# Election timer
_election_timer: threading.Timer | None = None
_timer_version: int = 0


def _save_raft_state() -> None:
    """Persist currentTerm + votedFor with fsync. Must hold _raft_lock."""
    if not DATA_DIR:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = _raft_path() + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"current_term": _current_term, "voted_for": _voted_for}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, _raft_path())


def _load_raft_state() -> None:
    global _current_term, _voted_for
    if not DATA_DIR or not os.path.exists(_raft_path()):
        return
    try:
        with open(_raft_path()) as f:
            s = json.load(f)
            _current_term = s.get("current_term", 0)
            _voted_for = s.get("voted_for", None)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Election timer helpers (must hold _raft_lock when calling)
# ---------------------------------------------------------------------------

def _reset_election_timer() -> None:
    global _election_timer, _timer_version
    _timer_version += 1
    version = _timer_version
    if _election_timer is not None:
        _election_timer.cancel()
    timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
    _election_timer = threading.Timer(timeout, _on_election_timeout, args=[version])
    _election_timer.daemon = True
    _election_timer.start()


def _cancel_election_timer() -> None:
    global _election_timer, _timer_version
    _timer_version += 1
    if _election_timer is not None:
        _election_timer.cancel()
        _election_timer = None


# ---------------------------------------------------------------------------
# Raft role transitions (must hold _raft_lock when calling)
# ---------------------------------------------------------------------------

def _step_down(new_term: int, new_leader: str | None = None) -> None:
    """Revert to follower. Only clears votedFor (and saves) when term increases."""
    global _current_term, _voted_for, _role, _leader
    prev_role = _role
    if new_term > _current_term:
        _current_term = new_term
        _voted_for = None
        _save_raft_state()
    _role = "follower"
    _leader = new_leader
    _reset_election_timer()
    log.info("step_down term=%d leader=%s (was %s)", _current_term, new_leader, prev_role)


def _become_leader() -> None:
    """Transition to leader. Must hold _raft_lock."""
    global _role, _leader
    _role = "leader"
    _leader = SELF_ADDR
    _cancel_election_timer()
    log.info("became leader term=%d", _current_term)
    # Immediately notify followers without waiting for the heartbeat loop.
    term = _current_term
    peers = list(PEERS)
    threading.Thread(target=_broadcast_heartbeat, args=(term, peers), daemon=True).start()


def _start_election() -> None:
    """Kick off a new election. Must hold _raft_lock."""
    global _current_term, _voted_for, _role, _leader, _votes_received
    _current_term += 1
    _role = "candidate"
    _voted_for = SELF_ADDR
    _leader = None
    _votes_received = {SELF_ADDR}
    _save_raft_state()
    _reset_election_timer()
    log.info("election started term=%d", _current_term)

    term = _current_term
    peers = list(PEERS)
    threading.Thread(target=_run_election, args=(term, peers), daemon=True).start()


# ---------------------------------------------------------------------------
# Election timeout callback
# ---------------------------------------------------------------------------

def _on_election_timeout(version: int) -> None:
    with _raft_lock:
        if version != _timer_version or _role == "leader":
            return
        _start_election()


# ---------------------------------------------------------------------------
# Outbound RPCs
# ---------------------------------------------------------------------------

def _rpc_request_vote(peer: str, term: int) -> tuple[bool, int]:
    try:
        resp = http.post(
            f"http://{peer}/raft/request-vote",
            json={
                "term": term,
                "candidate-id": SELF_ADDR,
                "last-log-index": 0,
                "last-log-term": 0,
            },
            timeout=RPC_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("vote-granted", False), data.get("term", 0)
    except Exception:
        pass
    return False, 0


def _rpc_append_entries(peer: str, term: int) -> tuple[bool, int]:
    try:
        resp = http.post(
            f"http://{peer}/raft/append-entries",
            json={
                "term": term,
                "leader-id": SELF_ADDR,
                "prev-log-index": 0,
                "prev-log-term": 0,
                "entries": [],
                "leader-commit": 0,
            },
            timeout=RPC_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("success", False), data.get("term", 0)
    except Exception:
        pass
    return False, 0


# ---------------------------------------------------------------------------
# Election runner (background thread — no lock held on entry)
# ---------------------------------------------------------------------------

def _run_election(term: int, peers: list[str]) -> None:
    cluster_size = len(peers) + 1
    quorum = cluster_size // 2 + 1

    futures: dict = {}
    with ThreadPoolExecutor(max_workers=max(len(peers), 1)) as ex:
        for peer in peers:
            futures[ex.submit(_rpc_request_vote, peer, term)] = peer

        for fut in as_completed(futures, timeout=ELECTION_TIMEOUT_MIN):
            peer = futures[fut]
            try:
                granted, peer_term = fut.result()
            except Exception:
                continue

            with _raft_lock:
                if _role != "candidate" or _current_term != term:
                    return
                if peer_term > _current_term:
                    _step_down(peer_term)
                    return
                if granted:
                    _votes_received.add(peer)
                    log.info("vote granted by %s term=%d votes=%d/%d",
                             peer, term, len(_votes_received), quorum)
                    if len(_votes_received) >= quorum:
                        _become_leader()
                        return


# ---------------------------------------------------------------------------
# Heartbeat sender
# ---------------------------------------------------------------------------

def _fire_heartbeat(peer: str, term: int) -> None:
    """Send a single fire-and-forget heartbeat. Only acts on higher-term responses."""
    _, peer_term = _rpc_append_entries(peer, term)
    if peer_term > term:
        with _raft_lock:
            if peer_term > _current_term:
                _step_down(peer_term)


def _broadcast_heartbeat(term: int, peers: list[str]) -> None:
    """Fire-and-forget heartbeats (used for immediate notification on leader election)."""
    for peer in peers:
        threading.Thread(target=_fire_heartbeat, args=(peer, term), daemon=True).start()


def _heartbeat_round(term: int, peers: list[str]) -> tuple[int, int]:
    """Send heartbeats to all peers, wait for responses.

    Returns (ack_count_including_self, max_peer_term).
    Exits early once quorum is confirmed to minimise latency.
    """
    if not peers:
        return 1, 0

    cluster_size = len(peers) + 1
    quorum = cluster_size // 2 + 1
    ack_count = 1  # self always acks
    max_term = 0

    with ThreadPoolExecutor(max_workers=len(peers)) as ex:
        futs = {ex.submit(_rpc_append_entries, p, term): p for p in peers}
        for f in as_completed(futs, timeout=RPC_TIMEOUT):
            try:
                ok, peer_term = f.result()
            except Exception:
                ok, peer_term = False, 0
            if peer_term > max_term:
                max_term = peer_term
            if ok:
                ack_count += 1
            if ack_count >= quorum:
                break  # No need to wait for remaining peers

    return ack_count, max_term


def _heartbeat_loop() -> None:
    """Daemon thread: synchronous heartbeat rounds every HEARTBEAT_INTERVAL.

    After each round: step down immediately if quorum was not achieved.
    This means partition detection latency = HEARTBEAT_INTERVAL + RPC_TIMEOUT.
    """
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with _raft_lock:
            if _role != "leader":
                continue
            term = _current_term
            peers = list(PEERS)

        ack_count, max_term = _heartbeat_round(term, peers)

        with _raft_lock:
            if _role != "leader" or _current_term != term:
                continue
            if max_term > _current_term:
                _step_down(max_term)
                continue
            cluster_size = len(PEERS) + 1
            quorum = cluster_size // 2 + 1
            if ack_count < quorum:
                log.info("quorum lost (%d/%d) term=%d — stepping down",
                         ack_count, quorum, _current_term)
                _step_down(_current_term)


# ---------------------------------------------------------------------------
# Flask: Raft RPC endpoints
# ---------------------------------------------------------------------------

@app.route("/raft/request-vote", methods=["POST"])
def raft_request_vote():
    global _current_term, _voted_for

    body = freq.get_json(force=True)
    cand_term = body["term"]
    cand_id = body["candidate-id"]

    with _raft_lock:
        if cand_term > _current_term:
            _step_down(cand_term)

        grant = (
            cand_term >= _current_term
            and (_voted_for is None or _voted_for == cand_id)
        )
        if grant:
            _voted_for = cand_id
            _save_raft_state()
            _reset_election_timer()
            log.info("voted for %s term=%d", cand_id, _current_term)

        return Response(
            json.dumps({"term": _current_term, "vote-granted": grant}),
            status=200, content_type="application/json",
        )


@app.route("/raft/append-entries", methods=["POST"])
def raft_append_entries():
    global _current_term, _voted_for, _role, _leader, _leader_last_contact

    body = freq.get_json(force=True)
    leader_term = body["term"]
    leader_id = body["leader-id"]

    with _raft_lock:
        if leader_term < _current_term:
            return Response(
                json.dumps({"term": _current_term, "success": False}),
                status=200, content_type="application/json",
            )

        if leader_term > _current_term:
            _step_down(leader_term, leader_id)
        else:
            if _role == "candidate":
                log.info("candidate yielding to leader %s term=%d", leader_id, leader_term)
                _role = "follower"
            _leader = leader_id
            _reset_election_timer()

        _leader_last_contact = time.monotonic()

        return Response(
            json.dumps({"term": _current_term, "success": True}),
            status=200, content_type="application/json",
        )


@app.route("/cluster/info", methods=["GET"])
def cluster_info():
    with _raft_lock:
        info = {
            "role": _role,
            "term": _current_term,
            "leader": _leader,
            "peers": sorted(PEERS),
        }
    return Response(json.dumps(info), status=200, content_type="application/json")


# ---------------------------------------------------------------------------
# Flask: health check
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return Response("ok\n", status=200)


# ---------------------------------------------------------------------------
# Flask: KV routes — leader handles, followers redirect, no-leader → 503
# ---------------------------------------------------------------------------

def _leader_check() -> Response | None:
    """Return a redirect/error response if we're not the leader, else None."""
    with _raft_lock:
        role = _role
        leader = _leader
        contact_age = time.monotonic() - _leader_last_contact

    if role == "leader":
        return None

    # Only redirect if we've heard from the leader recently.
    if leader is not None and contact_age <= LEADER_STALE_THRESHOLD:
        path = freq.full_path if freq.query_string else freq.path
        return Response(
            status=307,
            headers={"Location": f"http://{leader}{path}"},
        )

    return Response("no leader\n", status=503)


@app.route("/kv/", methods=["GET", "PUT", "DELETE"])
@app.route("/kv/<key>", methods=["GET", "PUT", "DELETE"])
def kv(key: str = ""):
    if not key:
        return Response("key cannot be empty\n", status=400)

    redir = _leader_check()
    if redir is not None:
        return redir

    if freq.method == "PUT":
        value = freq.get_data(as_text=True)
        if not value:
            return Response("value cannot be empty\n", status=400)
        if DATA_DIR:
            _append_wal({"op": "put", "key": key, "value": value})
        with kv_lock:
            store[key] = value
        return Response(status=200)

    elif freq.method == "GET":
        with kv_lock:
            value = store.get(key)
        if value is None:
            return Response("key not found\n", status=404)
        return Response(value, status=200)

    elif freq.method == "DELETE":
        if DATA_DIR:
            _append_wal({"op": "delete", "key": key})
        with kv_lock:
            store.pop(key, None)
        return Response(status=200)

    return Response("method not allowed\n", status=405)


@app.route("/clear", methods=["DELETE"])
def clear():
    redir = _leader_check()
    if redir is not None:
        return redir

    if DATA_DIR:
        _append_wal({"op": "clear"})
    with kv_lock:
        store.clear()
    return Response(status=200)


@app.errorhandler(405)
def method_not_allowed(_):
    return Response("method not allowed\n", status=405)


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

log.info("starting node=%s peers=%s", SELF_ADDR, PEERS)
_load_kv_data()
_load_raft_state()
atexit.register(_checkpoint)

threading.Thread(target=_heartbeat_loop, daemon=True).start()

with _raft_lock:
    _reset_election_timer()
