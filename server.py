"""
Distributed Key-Value Store server.

Entry point: python server.py [--data-dir=PATH]
This script parses args, sets environment, then execs gunicorn for
production-grade request handling and graceful shutdown.
"""

import atexit
import json
import os
import sys
import threading

from flask import Flask, request, Response

app = Flask(__name__)

store: dict[str, str] = {}
lock = threading.Lock()

DATA_DIR: str = os.environ.get("DATA_DIR", "")


def data_path() -> str:
    return os.path.join(DATA_DIR, "store.json")


def load_data() -> None:
    if not DATA_DIR:
        return
    path = data_path()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            loaded = json.load(f)
        with lock:
            store.update(loaded)


def save_data() -> None:
    if not DATA_DIR:
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    with lock:
        snapshot = dict(store)
    tmp = data_path() + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snapshot, f)
    os.replace(tmp, data_path())


# Load on import so gunicorn workers restore state after fork.
load_data()

# Save on exit so graceful shutdown (gunicorn SIGTERM) persists data.
atexit.register(save_data)


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
        with lock:
            store.pop(key, None)
        return Response(status=200)

    return Response("method not allowed\n", status=405)


@app.route("/clear", methods=["DELETE"])
def clear():
    with lock:
        store.clear()
    return Response(status=200)


@app.errorhandler(405)
def method_not_allowed(_):
    return Response("method not allowed\n", status=405)


if __name__ == "__main__":
    for i, arg in enumerate(sys.argv[1:], 1):
        if arg.startswith("--data-dir="):
            os.environ["DATA_DIR"] = arg.split("=", 1)[1]
        elif arg == "--data-dir" and i < len(sys.argv) - 1:
            os.environ["DATA_DIR"] = sys.argv[i + 1]

    os.execvp(
        "gunicorn",
        [
            "gunicorn",
            "--bind", "0.0.0.0:8080",
            "--workers", "1",
            "--worker-class", "gthread",
            "--threads", "128",
            "--graceful-timeout", "5",
            "--timeout", "30",
            "server:app",
        ],
    )
