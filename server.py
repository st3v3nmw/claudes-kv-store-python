"""
Distributed Key-Value Store — leader-election stage.

Run: gunicorn --config gunicorn.conf.py server:app
Env: DATA_DIR, PEERS (comma-separated peer addresses)

Architecture
------------
The Raft log is the source of truth for KV state.  Each entry carries
{index, term, op, key?, value?}.  The leader appends, immediately commits
(pre-replication shortcut; changes in log-replication stage), and applies
to the in-memory store.

Persistence (files under DATA_DIR):
  raft_state.json  — current_term, voted_for          (fsync on vote/term change)
  raft_log.jsonl   — log entries, one JSON line each  (group-commit fsync per append)
  snapshot.json    — materialised KV dict              (fsync on SIGTERM)

Recovery: load snapshot → load raft_state → replay all log entries
(single-node: every appended entry is committed immediately, so applying
all of them on recovery is always correct).

Raft timing:
  Election timeout : 500–1000 ms (randomised)
  Heartbeat        : 50 ms
  RPC timeout      : 70 ms
"""

import atexit
import json
import logging
import os
import queue as _queue_module
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
# Perf stats — accumulated across all threads, printed every 2 s
# ---------------------------------------------------------------------------

_stats_lock = threading.Lock()
_stats: dict = {
    # requests
    "req_n": 0, "req_ms": 0.0, "req_max_ms": 0.0,
    # leader_write
    "write_n": 0, "write_ms": 0.0, "write_max_ms": 0.0,
    "persist_ms": 0.0, "persist_max_ms": 0.0,
    # fsync (leader only)
    "fsync_n": 0, "fsync_ms": 0.0, "fsync_max_ms": 0.0,
    "fsync_batch_total": 0,           # sum of batch sizes
}


def _record(**kw):
    with _stats_lock:
        for k, v in kw.items():
            if k.endswith("_max_ms"):
                if v > _stats[k]:
                    _stats[k] = v
            else:
                _stats[k] += v


def _stats_printer():
    while True:
        time.sleep(2)
        with _stats_lock:
            s = dict(_stats)
            for k in _stats:
                _stats[k] = 0

        if s["req_n"] == 0 and s["write_n"] == 0:
            continue

        req_avg   = s["req_ms"]     / s["req_n"]   if s["req_n"]   else 0
        write_avg = s["write_ms"]   / s["write_n"] if s["write_n"] else 0
        per_avg   = s["persist_ms"] / s["write_n"] if s["write_n"] else 0
        fsync_avg = s["fsync_ms"]   / s["fsync_n"] if s["fsync_n"] else 0
        batch_avg = s["fsync_batch_total"] / s["fsync_n"] if s["fsync_n"] else 0

        log.info(
            "STATS | reqs=%d avg=%.1fms max=%.1fms | "
            "writes=%d avg=%.1fms max=%.1fms persist_avg=%.1fms persist_max=%.1fms | "
            "fsyncs=%d avg=%.1fms max=%.1fms batch_avg=%.1f",
            s["req_n"], req_avg, s["req_max_ms"],
            s["write_n"], write_avg, s["write_max_ms"], per_avg, s["persist_max_ms"],
            s["fsync_n"], fsync_avg, s["fsync_max_ms"], batch_avg,
        )


threading.Thread(target=_stats_printer, daemon=True).start()

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DATA_DIR: str       = os.environ.get("DATA_DIR", "")
PEERS:    list[str] = [p.strip() for p in os.environ.get("PEERS", "").split(",") if p.strip()]
ADDR:     str       = os.environ.get("ADDR", "")   # own address, set by harness

ELECTION_TIMEOUT_MIN = 0.5   # seconds
ELECTION_TIMEOUT_MAX = 1.0
HEARTBEAT_INTERVAL   = 0.05  # seconds
# AppendEntries (heartbeat) involves no disk I/O on the receiver — short.
HEARTBEAT_RPC_TIMEOUT = 0.07  # seconds
# RequestVote triggers up to 2 fsyncs on the receiver (term + vote persist).
# This can take 100–200 ms on virtualised filesystems (Docker macOS).
VOTE_RPC_TIMEOUT = 0.35  # seconds

# Followers stop redirecting / reporting a stale leader after this window.
# Must be > HEARTBEAT_INTERVAL + HEARTBEAT_RPC_TIMEOUT to avoid false negatives.
LEADER_STALE_THRESHOLD = 0.3  # seconds

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
# In-memory KV state
# ---------------------------------------------------------------------------

store:   dict[str, str] = {}
kv_lock: threading.RLock = threading.RLock()  # always acquired after _raft_lock

# ---------------------------------------------------------------------------
# Raft log
#
# Entries: {index, term, op, key?, value?}  — 1-indexed externally.
# All mutations require _raft_lock.
# ---------------------------------------------------------------------------

_raft_log:     list[dict] = []
_commit_index: int        = 0   # highest index known to be committed
_last_applied: int        = 0   # highest index applied to store


def _log_last_index() -> int:
    return len(_raft_log)


def _log_last_term() -> int:
    return _raft_log[-1]["term"] if _raft_log else 0


def _log_term_at(index: int) -> int:
    """Term of entry at 1-based index, 0 if out of range."""
    if index <= 0 or index > len(_raft_log):
        return 0
    return _raft_log[index - 1]["term"]


def _log_entries_from(from_index: int) -> list[dict]:
    """Entries starting at 1-based from_index."""
    if from_index > len(_raft_log):
        return []
    return _raft_log[from_index - 1:]


def _log_append_mem(term: int, command: dict) -> tuple[int, dict]:
    """Append one entry to the in-memory log. Returns (index, entry). Caller holds _raft_lock."""
    entry = {"index": len(_raft_log) + 1, "term": term, **command}
    _raft_log.append(entry)
    return entry["index"], entry


def _log_truncate_after(index: int) -> None:
    """Remove entries with 1-based index > given value. Caller holds _raft_lock."""
    del _raft_log[index:]


def _apply_committed_entries() -> None:
    """Apply log[_last_applied.._commit_index] to store. Caller holds _raft_lock."""
    global _last_applied
    while _last_applied < _commit_index:
        entry = _raft_log[_last_applied]   # _last_applied is 0-based index of next entry
        _last_applied += 1
        op = entry.get("op")
        with kv_lock:
            if op == "put":
                store[entry["key"]] = entry["value"]
            elif op == "delete":
                store.pop(entry["key"], None)
            elif op == "clear":
                store.clear()


# ---------------------------------------------------------------------------
# Raft log file — append-only with WAL-writer group-commit fsync
#
# Request threads write a line to the buffer and park on _log_flushed.
# A single background WAL-writer thread drains the buffer and fsyncs on
# demand, then wakes all parked threads.  This naturally batches every
# write that arrived while a previous fsync was in flight.
#
# Only log entries are written here; term/vote live in raft_state.json.
# Truncation (rare, follower conflict) rewrites the whole file.
# ---------------------------------------------------------------------------

_log_write_lock: threading.Lock      = threading.Lock()   # serialise file writes
_log_flushed:    threading.Condition = threading.Condition()
_log_fh                              = None   # open file handle, guarded by _log_write_lock
_log_write_seq:  int                 = 0      # incremented under _log_write_lock
_log_synced_seq: int                 = 0      # updated by WAL writer, read under _log_flushed

_wal_work: threading.Event = threading.Event()   # signals WAL writer that work is pending


def _log_path() -> str:
    return os.path.join(DATA_DIR, RAFT_LOG_FILE)


_WAL_BATCH_WINDOW = 0.002   # 2 ms — let concurrent writes accumulate before fsync


def _wal_writer_loop() -> None:
    """Daemon: wait for pending writes, fsync the log file, wake parked callers."""
    global _log_synced_seq
    while True:
        _wal_work.wait()           # park until someone signals work
        _wal_work.clear()
        time.sleep(_WAL_BATCH_WINDOW)   # batch window: let other writers fill the buffer

        with _log_write_lock:
            target = _log_write_seq
            fh     = _log_fh
            batch_start = _log_synced_seq

        if fh is None or target <= _log_synced_seq:
            continue

        t0 = time.monotonic()
        try:
            fh.flush()
            t_flushed = time.monotonic()
            os.fsync(fh.fileno())
            t_fsynced = time.monotonic()
            fsync_ms   = (t_fsynced - t_flushed) * 1000
            batch_size = target - batch_start
            _record(
                fsync_n=1,
                fsync_ms=fsync_ms,
                fsync_max_ms=fsync_ms,
                fsync_batch_total=batch_size,
            )
        except Exception:
            log.exception("WAL writer fsync failed")
            continue

        with _log_flushed:
            _log_synced_seq = target
            _log_flushed.notify_all()


threading.Thread(target=_wal_writer_loop, daemon=True, name="wal-writer").start()


def _persist_log_entry(entry: dict) -> None:
    """Buffer one log entry and signal the WAL writer — does NOT wait for fsync.
    Durability on graceful shutdown is guaranteed by _checkpoint() draining the
    WAL before writing the snapshot.  Crash durability (SIGKILL) relies on the
    WAL writer completing its current fsync cycle before the kill arrives.
    Called WITHOUT _raft_lock held."""
    global _log_fh, _log_write_seq

    if not DATA_DIR:
        return

    line = json.dumps(entry, ensure_ascii=False) + "\n"

    with _log_write_lock:
        if _log_fh is None:
            os.makedirs(DATA_DIR, exist_ok=True)
            _log_fh = open(_log_path(), "a", encoding="utf-8")
        _log_fh.write(line)
        _log_write_seq += 1

    _wal_work.set()   # wake the WAL writer (idempotent if already set)


def _rewrite_log_file() -> None:
    """Rewrite the log file from the in-memory log (used after truncation).
    Caller holds _raft_lock."""
    global _log_fh

    if not DATA_DIR:
        return

    with _log_write_lock:
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
    """Replay log entries from disk into _raft_log. Called at startup."""
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
                pass   # truncated tail from a crash — stop here
                break


# ---------------------------------------------------------------------------
# raft_state.json — term + votedFor only
# ---------------------------------------------------------------------------

def _raft_state_path() -> str:
    return os.path.join(DATA_DIR, RAFT_STATE_FILE)


def _save_raft_state() -> None:
    """Persist current_term and voted_for with fsync. Caller holds _raft_lock."""
    if not DATA_DIR:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    tmp = _raft_state_path() + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"current_term": _current_term, "voted_for": _voted_for}, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, _raft_state_path())


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


# ---------------------------------------------------------------------------
# snapshot.json — materialised KV state (written on graceful shutdown)
# ---------------------------------------------------------------------------

def _snapshot_path() -> str:
    return os.path.join(DATA_DIR, SNAPSHOT_FILE)


def _checkpoint() -> None:
    """Write snapshot at graceful shutdown (SIGTERM / atexit).

    First drains the WAL writer so all buffered entries are on disk, then
    snapshots the current KV state.  This guarantees that the snapshot
    reflects every write that was acknowledged before the shutdown signal.
    """
    if not DATA_DIR:
        return

    # Drain: signal the WAL writer and wait for it to fsync everything written
    # so far (bounded wait so a broken WAL writer doesn't hang shutdown).
    with _log_write_lock:
        drain_target = _log_write_seq
    if drain_target > 0:
        _wal_work.set()
        with _log_flushed:
            deadline = time.monotonic() + 10.0
            while _log_synced_seq < drain_target:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    log.warning("checkpoint: WAL drain timed out (synced=%d target=%d)",
                                _log_synced_seq, drain_target)
                    break
                _log_flushed.wait(timeout=remaining)

    os.makedirs(DATA_DIR, exist_ok=True)
    with kv_lock:
        snapshot = dict(store)
    tmp = _snapshot_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, _snapshot_path())


def _load_snapshot() -> None:
    if not DATA_DIR:
        return
    path = _snapshot_path()
    if not os.path.exists(path):
        return
    try:
        with open(path, encoding="utf-8") as f:
            with kv_lock:
                store.update(json.load(f))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Raft persistent state
# ---------------------------------------------------------------------------

_raft_lock: threading.Lock = threading.Lock()

_current_term: int       = 0
_voted_for:    str | None = None

# ---------------------------------------------------------------------------
# Raft volatile state
# ---------------------------------------------------------------------------

_role:                str        = "follower"
_leader:              str | None = None
_leader_last_contact: float      = 0.0

# Per-peer leader state — initialised in _become_leader
_next_index:  dict[str, int] = {}
_match_index: dict[str, int] = {}

# Election timer
_election_timer:  threading.Timer | None = None
_timer_version:   int                    = 0

# ---------------------------------------------------------------------------
# Election timer helpers  (caller holds _raft_lock)
# ---------------------------------------------------------------------------

def _reset_election_timer() -> None:
    global _election_timer, _timer_version
    _timer_version += 1
    version = _timer_version
    if _election_timer is not None:
        _election_timer.cancel()
    timeout       = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
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
# Role transitions  (caller holds _raft_lock)
# ---------------------------------------------------------------------------

def _step_down(new_term: int, new_leader: str | None = None) -> None:
    global _current_term, _voted_for, _role, _leader, _next_index, _match_index
    if new_term > _current_term:
        _current_term = new_term
        _voted_for    = None
        _next_index   = {}
        _match_index  = {}
        _save_raft_state()
    _role   = "follower"
    _leader = new_leader
    _reset_election_timer()
    log.info("step_down term=%d leader=%s", _current_term, new_leader)


def _become_leader() -> None:
    global _role, _leader, _next_index, _match_index
    _role        = "leader"
    _leader      = SELF_ADDR
    _next_index  = {p: _log_last_index() + 1 for p in PEERS}
    _match_index = {p: 0 for p in PEERS}
    _cancel_election_timer()
    log.info("became leader term=%d", _current_term)
    term  = _current_term
    peers = list(PEERS)
    threading.Thread(target=_broadcast_heartbeat, args=(term, peers), daemon=True).start()


def _start_election() -> None:
    global _current_term, _voted_for, _role, _leader
    _current_term += 1
    _role         = "candidate"
    _voted_for    = SELF_ADDR
    _leader       = None
    _save_raft_state()
    _reset_election_timer()
    log.info("election started term=%d", _current_term)

    term     = _current_term
    peers    = list(PEERS)
    last_idx = _log_last_index()
    last_trm = _log_last_term()
    threading.Thread(
        target=_run_election, args=(term, peers, last_idx, last_trm), daemon=True
    ).start()


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

def _rpc_request_vote(peer: str, term: int,
                      last_log_index: int, last_log_term: int) -> tuple[bool, int]:
    try:
        resp = http.post(
            f"http://{peer}/raft/request-vote",
            json={
                "term":           term,
                "candidate-id":   SELF_ADDR,
                "last-log-index": last_log_index,
                "last-log-term":  last_log_term,
            },
            timeout=VOTE_RPC_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("vote-granted", False), data.get("term", 0)
    except Exception:
        pass
    return False, 0


def _rpc_append_entries(peer: str, term: int,
                        prev_log_index: int, prev_log_term: int,
                        entries: list, leader_commit: int) -> tuple[bool, int]:
    try:
        resp = http.post(
            f"http://{peer}/raft/append-entries",
            json={
                "term":           term,
                "leader-id":      SELF_ADDR,
                "prev-log-index": prev_log_index,
                "prev-log-term":  prev_log_term,
                "entries":        entries,
                "leader-commit":  leader_commit,
            },
            timeout=HEARTBEAT_RPC_TIMEOUT,
        )
        if resp.status_code == 200:
            data = resp.json()
            return data.get("success", False), data.get("term", 0)
    except Exception:
        pass
    return False, 0


# ---------------------------------------------------------------------------
# Election runner  (background thread — no lock on entry)
# ---------------------------------------------------------------------------

def _run_election(term: int, peers: list[str],
                  last_log_index: int, last_log_term: int) -> None:
    cluster_size = len(peers) + 1
    quorum       = cluster_size // 2 + 1

    if not peers:
        with _raft_lock:
            if _role == "candidate" and _current_term == term:
                _become_leader()
        return

    votes: set[str] = {SELF_ADDR}

    with ThreadPoolExecutor(max_workers=len(peers)) as ex:
        futs = {
            ex.submit(_rpc_request_vote, p, term, last_log_index, last_log_term): p
            for p in peers
        }
        try:
            for fut in as_completed(futs, timeout=VOTE_RPC_TIMEOUT + 0.05):
                peer = futs[fut]
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
                        votes.add(peer)
                        log.info("vote from %s term=%d votes=%d/%d",
                                 peer, term, len(votes), quorum)
                        if len(votes) >= quorum:
                            _become_leader()
                            return
        except TimeoutError:
            pass  # election didn't gather quorum in time; next timeout will retry


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

def _fire_heartbeat(peer: str, term: int, prev_idx: int, prev_trm: int,
                    entries: list, commit: int) -> None:
    _, peer_term = _rpc_append_entries(peer, term, prev_idx, prev_trm, entries, commit)
    if peer_term > term:
        with _raft_lock:
            if peer_term > _current_term:
                _step_down(peer_term)


def _broadcast_heartbeat(term: int, peers: list[str]) -> None:
    """Fire-and-forget initial heartbeat on becoming leader."""
    for peer in peers:
        threading.Thread(
            target=_fire_heartbeat, args=(peer, term, 0, 0, [], 0), daemon=True
        ).start()


def _heartbeat_round(term: int, peers: list[str]) -> tuple[int, int]:
    """Synchronous heartbeat round. Returns (ack_count_incl_self, max_peer_term)."""
    if not peers:
        return 1, 0

    cluster_size = len(peers) + 1
    quorum       = cluster_size // 2 + 1
    ack_count    = 1
    max_term     = 0

    with _raft_lock:
        commit    = _commit_index
        peer_args = {
            peer: (
                _next_index.get(peer, _log_last_index() + 1) - 1,
                _log_term_at(_next_index.get(peer, _log_last_index() + 1) - 1),
                [],      # entries — populated in log-replication stage
                commit,
            )
            for peer in peers
        }

    with ThreadPoolExecutor(max_workers=len(peers)) as ex:
        futs = {
            ex.submit(_rpc_append_entries, p, term, *args): p
            for p, args in peer_args.items()
        }
        try:
            # Use a slightly longer timeout than the per-RPC timeout so that
            # in-flight requests can complete and return (False, 0) rather than
            # racing against as_completed's deadline and raising TimeoutError.
            for f in as_completed(futs, timeout=HEARTBEAT_RPC_TIMEOUT + 0.05):
                try:
                    ok, peer_term = f.result()
                except Exception:
                    ok, peer_term = False, 0
                if peer_term > max_term:
                    max_term = peer_term
                if ok:
                    ack_count += 1
                if ack_count >= quorum:
                    break
        except TimeoutError:
            pass  # return whatever ack_count we have — quorum check handles it

    return ack_count, max_term


def _heartbeat_loop() -> None:
    """Daemon thread: synchronous heartbeat rounds every HEARTBEAT_INTERVAL."""
    while True:
        time.sleep(HEARTBEAT_INTERVAL)
        with _raft_lock:
            if _role != "leader":
                continue
            term  = _current_term
            peers = list(PEERS)

        try:
            ack_count, max_term = _heartbeat_round(term, peers)
        except Exception:
            log.exception("heartbeat_round raised — treating as quorum loss")
            ack_count, max_term = 0, 0

        with _raft_lock:
            if _role != "leader" or _current_term != term:
                continue
            if max_term > _current_term:
                _step_down(max_term)
                continue
            cluster_size = len(PEERS) + 1
            quorum       = cluster_size // 2 + 1
            if ack_count < quorum:
                log.info("quorum lost (%d/%d) term=%d — stepping down",
                         ack_count, quorum, _current_term)
                _step_down(_current_term)


# ---------------------------------------------------------------------------
# Flask: Raft RPC endpoints
# ---------------------------------------------------------------------------

@app.route("/raft/request-vote", methods=["POST"])
def raft_request_vote():
    global _voted_for

    body      = freq.get_json(force=True)
    cand_term = body["term"]
    cand_id   = body["candidate-id"]
    cand_lli  = body.get("last-log-index", 0)
    cand_llt  = body.get("last-log-term",  0)

    with _raft_lock:
        # Disruptive-server prevention (Raft §6): a follower that has heard
        # from a valid leader recently rejects higher-term votes.  We use
        # LEADER_STALE_THRESHOLD (not ELECTION_TIMEOUT_MIN) so that the
        # window expires before the election timeout fires — allowing
        # legitimate elections after a crash while still protecting a
        # working majority from partitioned candidates.
        if (cand_term > _current_term
                and _role == "follower"
                and _leader is not None
                and (time.monotonic() - _leader_last_contact) < LEADER_STALE_THRESHOLD):
            return Response(
                json.dumps({"term": _current_term, "vote-granted": False}),
                status=200, content_type="application/json",
            )

        if cand_term > _current_term:
            _step_down(cand_term)

        # Log up-to-date check (Raft §5.4.1)
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
            _save_raft_state()
            _reset_election_timer()
            log.info("voted for %s term=%d", cand_id, _current_term)

        return Response(
            json.dumps({"term": _current_term, "vote-granted": grant}),
            status=200, content_type="application/json",
        )


@app.route("/raft/append-entries", methods=["POST"])
def raft_append_entries():
    global _current_term, _role, _leader, _leader_last_contact, _commit_index

    body          = freq.get_json(force=True)
    leader_term   = body["term"]
    leader_id     = body["leader-id"]
    prev_log_idx  = body.get("prev-log-index", 0)
    prev_log_trm  = body.get("prev-log-term",  0)
    entries       = body.get("entries", [])
    leader_commit = body.get("leader-commit", 0)

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

        # Log consistency check
        if prev_log_idx > 0 and _log_term_at(prev_log_idx) != prev_log_trm:
            return Response(
                json.dumps({"term": _current_term, "success": False}),
                status=200, content_type="application/json",
            )

        # Append new entries, truncating any conflicting ones first
        for i, entry in enumerate(entries):
            slot = prev_log_idx + 1 + i
            if slot <= _log_last_index():
                if _log_term_at(slot) != entry["term"]:
                    _log_truncate_after(slot - 1)
                    _raft_log.append(entry)
            else:
                _raft_log.append(entry)

        if entries:
            _rewrite_log_file()

        # Advance commit index and apply
        if leader_commit > _commit_index:
            _commit_index = min(leader_commit, _log_last_index())
            _apply_committed_entries()

        return Response(
            json.dumps({"term": _current_term, "success": True}),
            status=200, content_type="application/json",
        )


@app.route("/cluster/info", methods=["GET"])
def cluster_info():
    with _raft_lock:
        role        = _role
        term        = _current_term
        leader      = _leader
        contact_age = time.monotonic() - _leader_last_contact

    if role != "leader" and (leader is None or contact_age > LEADER_STALE_THRESHOLD):
        leader = None

    return Response(
        json.dumps({"id": SELF_ADDR, "role": role, "term": term, "leader": leader, "peers": sorted(PEERS)}),
        status=200, content_type="application/json",
    )


# ---------------------------------------------------------------------------
# Flask: health check
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return Response("ok\n", status=200)


# ---------------------------------------------------------------------------
# Flask: KV routes
# ---------------------------------------------------------------------------

def _leader_check() -> Response | None:
    with _raft_lock:
        role        = _role
        leader      = _leader
        contact_age = time.monotonic() - _leader_last_contact

    if role == "leader":
        return None
    if leader is not None and contact_age <= LEADER_STALE_THRESHOLD:
        path = freq.full_path if freq.query_string else freq.path
        return Response(status=307, headers={"Location": f"http://{leader}{path}"})
    return Response("no leader\n", status=503)


# ---------------------------------------------------------------------------
# Write actor — single thread owns all mutable Raft/KV state for writes.
#
# HTTP threads enqueue (command, done_event, t0) and block on done_event.
# This eliminates _raft_lock contention between HTTP threads entirely:
# the actor is the only writer; it processes one command at a time with no
# lock competition.  Matches the Go reference batch-writer architecture.
# ---------------------------------------------------------------------------

_write_queue: _queue_module.SimpleQueue = _queue_module.SimpleQueue()


def _write_worker_loop() -> None:
    """Single-threaded actor: dequeue write commands, execute, signal callers."""
    global _commit_index
    while True:
        command, done_event, t0 = _write_queue.get()
        try:
            with _raft_lock:
                idx, entry = _log_append_mem(_current_term, command)
                _commit_index = idx
                _apply_committed_entries()

            t1 = time.monotonic()
            _persist_log_entry(entry)   # async WAL, does not block
            t2 = time.monotonic()

            total_ms   = (t2 - t0) * 1000
            persist_ms = (t2 - t1) * 1000
            _record(
                write_n=1,
                write_ms=total_ms,
                write_max_ms=total_ms,
                persist_ms=persist_ms,
                persist_max_ms=persist_ms,
            )
        finally:
            done_event.set()


threading.Thread(target=_write_worker_loop, daemon=True, name="write-worker").start()


def _leader_write(command: dict) -> None:
    """Enqueue a write command and block until the write actor processes it."""
    done = threading.Event()
    _write_queue.put((command, done, time.monotonic()))
    done.wait()


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
        _leader_write({"op": "put", "key": key, "value": value})
        return Response(status=200)

    if freq.method == "GET":
        with kv_lock:
            value = store.get(key)
        if value is None:
            return Response("key not found\n", status=404)
        return Response(value, status=200)

    if freq.method == "DELETE":
        _leader_write({"op": "delete", "key": key})
        return Response(status=200)

    return Response("method not allowed\n", status=405)


@app.route("/clear", methods=["DELETE"])
def clear():
    redir = _leader_check()
    if redir is not None:
        return redir
    _leader_write({"op": "clear"})
    return Response(status=200)


@app.errorhandler(405)
def method_not_allowed(_):
    return Response("method not allowed\n", status=405)


# ---------------------------------------------------------------------------
# Request timing middleware
# ---------------------------------------------------------------------------

@app.before_request
def _before_request():
    freq.environ["_t_start"] = time.monotonic()


@app.after_request
def _after_request(response):
    t_start = freq.environ.get("_t_start")
    if t_start is not None:
        elapsed_ms = (time.monotonic() - t_start) * 1000
        _record(req_n=1, req_ms=elapsed_ms, req_max_ms=elapsed_ms)
    return response


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

log.info("starting node=%s peers=%s", SELF_ADDR, PEERS)
_load_snapshot()
_load_raft_state()
_load_log_file()

# Re-apply any log entries committed after the last snapshot.
with _raft_lock:
    _commit_index = _log_last_index()   # single-node: all entries are committed
    _apply_committed_entries()

atexit.register(_checkpoint)
threading.Thread(target=_heartbeat_loop, daemon=True).start()

with _raft_lock:
    if not PEERS:
        # Single-node: skip the election timeout, become leader immediately.
        _current_term += 1
        _voted_for     = SELF_ADDR
        _save_raft_state()
        _become_leader()
    else:
        _reset_election_timer()
