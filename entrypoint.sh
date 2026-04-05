#!/bin/sh
exec gunicorn --config gunicorn.conf.py server:app
