#!/bin/bash

set -eo pipefail

# Load the model at startup in server processes only; everything else that
# sets up Django (tests, management commands) loads lazily on first use.
export SELDON_WARMUP="${SELDON_WARMUP:-1}"

# Start server
if [[ $DEV_MODE == 1 ]]; then
  exec python manage.py runserver 0.0.0.0:${WEB_APP_PORT:-8000}
else
  exec gunicorn seldon_project.wsgi --bind 0.0.0.0:${WEB_APP_PORT:-8000} --workers=${GUNICORN_WORKERS:=1}
fi
