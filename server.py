import threading
from flask import Flask, request, Response

app = Flask(__name__)

store: dict[str, str] = {}
lock = threading.Lock()


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
    app.run(host="0.0.0.0", port=8080, threaded=True)
