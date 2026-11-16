#!/bin/bash

set -eo pipefail

# Start server
if [[ $DEV_MODE == 1 ]]; then
  exec python manage.py runserver 0.0.0.0:${WEB_APP_PORT:-8000}
else
  exec gunicorn seldon_project.wsgi --bind 0.0.0.0:${WEB_APP_PORT:-8000} --workers=${GUNICORN_WORKERS:=1}
fi
