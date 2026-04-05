#!/bin/sh
# Parse --data-dir and --peers args, export as env vars, then run gunicorn.
for arg in "$@"; do
    case "$arg" in
        --data-dir=*) export DATA_DIR="${arg#*=}" ;;
        --peers=*)    export PEERS="${arg#*=}" ;;
    esac
done

exec gunicorn --config gunicorn.conf.py server:app
