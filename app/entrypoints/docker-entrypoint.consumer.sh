#!/bin/bash

set -eo pipefail

# Bring the database schema up to date; nothing else migrates on `up`.
python manage.py migrate --noinput

# The command loads the model itself, once, and exits if that fails.
exec python manage.py consume_alerts
