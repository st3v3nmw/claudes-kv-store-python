"""
Distributed Key-Value Store server.

Run with gunicorn: gunicorn --config gunicorn.conf.py server:app
Or directly:       DATA_DIR=./data python server.py

Persistence strategy:
  - WAL (write-ahead log): every mutating operation is written to wal.jsonl.
    Group-commit logic batches concurrent writes into a single fsync so
    throughput under load stays high while durability is never compromised.
  - Snapshot: written on graceful shutdown. On startup, snapshot + WAL are
    replayed to restore full state.
"""

import atexit
import json
import os
import threading

from flask import Flask, request, Response

app = Flask(__name__)

store: dict[str, str] = {}
lock = threading.Lock()

DATA_DIR: str = os.environ.get("DATA_DIR", "")

WAL_FILE = "wal.jsonl"
SNAPSHOT_FILE = "snapshot.json"

# ---------------------------------------------------------------------------
# WAL with group-commit
#
# _wal_cond protects all WAL state below.
# Write flow:
#   1. Acquire _wal_cond (mutex), write line, get my_seq.
#   2. If another thread is syncing: wait on _wal_cond until _synced_seq
#      covers my write, then return.
#   3. Otherwise: become the syncer, save a local fh ref, release mutex.
#   4. Flush + fsync (slow — outside the mutex so other threads can write).
#   5. Re-acquire mutex, update _synced_seq, notify all waiters.
#
# Multiple concurrent writes accumulate while step 4 runs; the next syncer
# covers them all in one fsync — this is the group-commit effect.
# ---------------------------------------------------------------------------

_wal_cond = threading.Condition()
_wal_fh = None
_write_seq: int = 0
_synced_seq: int = 0
_sync_in_progress: bool = False


def snapshot_path() -> str:
    return os.path.join(DATA_DIR, SNAPSHOT_FILE)


def wal_path() -> str:
    return os.path.join(DATA_DIR, WAL_FILE)


def _append_wal(entry: dict) -> None:
    global _wal_fh, _write_seq, _synced_seq, _sync_in_progress

    line = json.dumps(entry, ensure_ascii=False) + "\n"
    fh = None
    target = 0

    with _wal_cond:
        if _wal_fh is None:
            os.makedirs(DATA_DIR, exist_ok=True)
            _wal_fh = open(wal_path(), "a", encoding="utf-8")

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
    """Write snapshot and truncate WAL. Safe to call at shutdown."""
    global _wal_fh

    if not DATA_DIR:
        return

    os.makedirs(DATA_DIR, exist_ok=True)

    with lock:
        snapshot = dict(store)

    tmp = snapshot_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False)
    os.replace(tmp, snapshot_path())

    with _wal_cond:
        if _wal_fh is not None:
            _wal_fh.close()
            _wal_fh = None

    open(wal_path(), "w").close()


def load_data() -> None:
    """Replay snapshot + WAL into in-memory store."""
    if not DATA_DIR:
        return

    if os.path.exists(snapshot_path()):
        with open(snapshot_path(), "r", encoding="utf-8") as f:
            try:
                store.update(json.load(f))
            except json.JSONDecodeError:
                pass

    if os.path.exists(wal_path()):
        with open(wal_path(), "r", encoding="utf-8") as f:
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
# Flask routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return Response("ok\n", status=200)


@app.route("/kv/", methods=["GET", "PUT", "DELETE"])
@app.route("/kv/<key>", methods=["GET", "PUT", "DELETE"])
def kv(key: str = ""):
    if not key:
        return Response("key cannot be empty\n", status=400)

    if request.method == "PUT":
        value = request.get_data(as_text=True)
        if not value:
            return Response("value cannot be empty\n", status=400)
        if DATA_DIR:
            _append_wal({"op": "put", "key": key, "value": value})
        with lock:
            store[key] = value
        return Response(status=200)

    elif request.method == "GET":
        with lock:
            value = store.get(key)
        if value is None:
            return Response("key not found\n", status=404)
        return Response(value, status=200)

    elif request.method == "DELETE":
        if DATA_DIR:
            _append_wal({"op": "delete", "key": key})
        with lock:
            store.pop(key, None)
        return Response(status=200)

    return Response("method not allowed\n", status=405)


@app.route("/clear", methods=["DELETE"])
def clear():
    if DATA_DIR:
        _append_wal({"op": "clear"})
    with lock:
        store.clear()
    return Response(status=200)


@app.errorhandler(405)
def method_not_allowed(_):
    return Response("method not allowed\n", status=405)


# ---------------------------------------------------------------------------
# Startup and shutdown
# ---------------------------------------------------------------------------

load_data()
atexit.register(_checkpoint)
